# Payment Processing Service

Асинхронный сервис принимает платежи, сохраняет их в PostgreSQL, передаёт на обработку через RabbitMQ и отправляет результат клиенту webhook-запросом. Внешний платёжный шлюз эмулируется: результат обработки определяется случайно с заданной вероятностью.

## Технологии

- Python 3.12+, FastAPI и Pydantic v2
- SQLAlchemy 2.0 в async-режиме и PostgreSQL
- RabbitMQ и FastStream
- Alembic для миграций
- HTTPX для webhook-запросов
- Docker Compose для локального запуска

## Структура проекта

```text
src/payment_service/
  api/routes.py          API, идемпотентность и создание платежа
  config.py              настройки приложения
  models.py              ORM-модели платежей, outbox и webhook-доставки
  schemas.py             схемы запросов и ответов
  worker/broker.py       очереди RabbitMQ
  worker/consumer.py     consumer и фоновые задачи
  worker/services.py     outbox, обработка платежей и webhook
migrations/              конфигурация Alembic и миграции
tests/                   тесты API и сервисной логики
```

## Запуск в Docker Compose

Для запуска требуются Docker Engine и Docker Compose v2. Перед первым запуском необходимо создать `.env` на основе `.env.example`. В `API_KEY` требуется указать случайный секрет длиной не менее 16 символов. В переменных `POSTGRES_PASSWORD` и `RABBITMQ_PASSWORD` также должны быть заданы собственные пароли; `RABBITMQ_USER` задаёт имя пользователя RabbitMQ.

```powershell
Copy-Item .env.example .env
```

После настройки запускаются PostgreSQL, RabbitMQ, миграция, API и consumer:

```powershell
docker compose up --build -d
```

API доступен по адресу `http://localhost:8000`. RabbitMQ Management UI опубликован на `http://localhost:15672`; имя пользователя и пароль берутся из `.env`. PostgreSQL и AMQP-порт RabbitMQ доступны сервисам внутри Compose-сети. Миграция применяется одноразовым сервисом `migrate` до старта API и consumer.

Для повторного применения миграций используется команда:

```powershell
docker compose run --rm migrate alembic upgrade head
```

Просмотр логов и остановка сервисов:

```powershell
docker compose logs -f api consumer
docker compose down
```

## Локальный запуск

При локальном запуске Python-процессов PostgreSQL и RabbitMQ должны быть доступны по адресам из `DATABASE_URL` и `RABBITMQ_URL`. Зависимости устанавливаются из корня проекта:

```powershell
uv pip install --system -e .[test]
$env:PYTHONPATH = "src"
```

Миграции применяются командой:

```powershell
alembic upgrade head
```

API и consumer запускаются в отдельных терминалах:

```powershell
uvicorn payment_service.main:app --app-dir src --reload
```

```powershell
$env:PYTHONPATH = "src"
python -m payment_service.worker.consumer
```

## Настройки

Все параметры перечислены в `.env.example`. `API_KEY` обязателен; API сравнивает его со значением заголовка `X-API-Key` в постоянное время. В исходном коде секреты не хранятся.

По умолчанию обработка платежа занимает от 2 до 5 секунд, вероятность успеха составляет 90%. Диапазон задержки, вероятность успеха, таймаут webhook и интервалы фоновых задач настраиваются через переменные окружения.

## API

Платёжные endpoint требуют заголовок `X-API-Key`. Для создания платежа также обязателен `Idempotency-Key`.

```http
POST /api/v1/payments
X-API-Key: <api-key>
Idempotency-Key: order-2026-0042
Content-Type: application/json

{
  "amount": "1250.00",
  "currency": "RUB",
  "description": "Оплата заказа 42",
  "metadata": {"order_id": 42},
  "webhook_url": "https://merchant.example/hooks/payment"
}
```

Первый запрос возвращает `202 Accepted`:

```json
{
  "payment_id": "e58136a6-81a0-4f46-b02c-824fffb605ce",
  "status": "pending",
  "created_at": "2026-10-30T12:00:00Z"
}
```

Повтор с тем же ключом и теми же данными возвращает тот же платёж и статус `202`. Если тело запроса отличается, сервер отвечает `409 Conflict`. Порядок ключей внутри `metadata` на проверку идемпотентности не влияет.

Получение сведений о платеже:

```http
GET /api/v1/payments/e58136a6-81a0-4f46-b02c-824fffb605ce
X-API-Key: <api-key>
```

Ответ содержит UUID, сумму, валюту, описание, `metadata`, статус, webhook URL, `created_at` и `processed_at`. Для неизвестного UUID возвращается `404 Not Found`. `/health` проверяет доступность API и PostgreSQL и используется healthcheck контейнера; endpoint не входит в платежный API.

## Обработка платежей и доставка событий

### Outbox

Создание платежа и запись события `payment.requested` выполняются в одной транзакции PostgreSQL. Фоновый publisher выбирает неопубликованные записи через `FOR UPDATE SKIP LOCKED`, устанавливает lease и отправляет persistent-сообщения в `payments.new`. После подтверждения RabbitMQ запись помечается опубликованной.

При временной ошибке lease снимается, а следующая попытка откладывается с экспоненциальной задержкой. Если процесс завершится после публикации, но до отметки в базе, RabbitMQ может получить событие повторно. Consumer проверяет статус платежа и не обрабатывает завершённый платёж снова.

### Consumer, повторы и DLQ

Consumer FastStream блокирует строку платежа и обрабатывает только платежи со статусом `pending`. Итоговый статус и запись для webhook сохраняются атомарно. Статус `failed` является завершённым результатом эмулированного платежа и не приводит к повторной обработке.

Для временных ошибок предусмотрены три попытки обработки: первоначальная доставка и два повтора. Повторы проходят через durable-очереди `payments.retry.1` и `payments.retry.2`. RabbitMQ возвращает их в `payments.new` через 1 и 2 секунды соответственно. После третьей неудачи сообщение попадает в `payments.dlq`. Consumer объявляет retry-очереди и DLQ при старте; FastStream объявляет `payments.new`. Сообщение содержит идентификатор платежа и номер попытки. Очереди durable, поэтому сообщения сохраняются при перезапуске RabbitMQ.

### Webhook

После сохранения результата платежа в `webhook_deliveries` создаётся отдельная запись на отправку уведомления. Ошибки сети и ответы с кодом вне диапазона 2xx повторяются с экспоненциальной задержкой; пауза между попытками ограничена одним часом. Повторы webhook не меняют статус платежа и не запускают его обработку заново. Общее число попыток webhook не ограничено.

В запрос добавляется заголовок `X-Webhook-Event-Id` со стабильным UUID события. Получатель может использовать его для дедупликации. Тело webhook:

```json
{
  "payment_id": "e58136a6-81a0-4f46-b02c-824fffb605ce",
  "status": "succeeded",
  "amount": "1250.00",
  "currency": "RUB",
  "processed_at": "2026-10-30T12:00:04Z"
}
```

Доставка выполняется как минимум один раз. Если удалённый сервис принял запрос, а процесс завершился до сохранения подтверждения, webhook может быть отправлен повторно.

## Миграции и тесты

В Compose миграции применяются сервисом `migrate`. Для локального запуска используется `alembic upgrade head`.

Тесты запускаются из корня проекта:

```powershell
$env:PYTHONPATH = "src"
pytest
ruff check src tests migrations
```

Pytest использует SQLite in-memory для API и сервисной логики. Публикация в outbox проверяется с mock broker; webhook тестируется через HTTPX `MockTransport`. Отдельная проверка PostgreSQL, RabbitMQ, TTL/DLX и миграции выполняется через Docker Compose.

## Ограничения

- Платёжный шлюз эмулируется; реальные списания не выполняются.
- Webhook-доставка имеет гарантию at-least-once, а не exactly-once. Получателю следует дедуплицировать запросы по `X-Webhook-Event-Id`.
