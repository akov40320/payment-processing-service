# Payment Processing Service

Асинхронный сервис принимает платежи, сохраняет запрос в PostgreSQL, передаёт его обработчику через RabbitMQ и уведомляет клиента о результате. Платёжный шлюз в этом задании эмулируется задержкой и настраиваемой вероятностью успеха.

## Стек и структура

- Python 3.12+, FastAPI, Pydantic v2.
- SQLAlchemy 2 async, PostgreSQL, Alembic.
- RabbitMQ и FastStream.
- HTTPX для исходящих webhook-запросов.

```text
src/payment_service/
  api/routes.py          HTTP API, idempotency и транзакция создания
  config.py              настройки окружения
  models.py              ORM-модели payment, outbox, webhook_deliveries
  schemas.py             входные и выходные схемы
  worker/broker.py       очередь, retry-очереди и DLQ
  worker/consumer.py     consumer и жизненный цикл фоновых задач
  worker/services.py     публикация outbox, обработка платежа, webhook
migrations/              Alembic environment и миграции
tests/                   API и сервисные тесты
```

## Запуск

Требуется Docker с Compose v2. Создайте `.env` из шаблона и замените `API_KEY` на случайный секрет длиной не менее 16 символов. Для собственного окружения также задайте `POSTGRES_PASSWORD`, `RABBITMQ_USER` и `RABBITMQ_PASSWORD`.

```powershell
Copy-Item .env.example .env
docker compose up --build -d
```

Сервис API доступен на `http://localhost:8000`. PostgreSQL и RabbitMQ доступны внутри Compose-сети; RabbitMQ Management UI опубликован на `http://localhost:15672` (учётные данные задаются `RABBITMQ_USER` и `RABBITMQ_PASSWORD`). Миграция выполняется одноразовым сервисом `migrate` перед запуском API и consumer.

Команды обслуживания:

```powershell
docker compose run --rm migrate alembic upgrade head
docker compose logs -f api consumer
docker compose down
```

Для локального запуска приложения после установки зависимостей нужно задать `DATABASE_URL`, `RABBITMQ_URL` и `API_KEY`, затем выполнить `alembic upgrade head`, `uvicorn payment_service.main:app --app-dir src --reload` и отдельно `python -m payment_service.worker.consumer` с `PYTHONPATH=src`.

## Настройки

Полный список переменных находится в `.env.example`. Обязательный `API_KEY` сравнивается в постоянное время через `X-API-Key`. Секреты не включены в исходный код. Конфигурация задаёт диапазон задержки обработки, вероятность успеха, таймаут webhook и интервалы фоновых опросов. Значения платежа по умолчанию: задержка 2–5 секунд и вероятность успеха 90%.

## API

Оба платежных endpoint требуют `X-API-Key`. Для создания также обязателен `Idempotency-Key`.

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

Ответ при первом создании — `202 Accepted`:

```json
{
  "payment_id": "e58136a6-81a0-4f46-b02c-824fffb605ce",
  "status": "pending",
  "created_at": "2026-10-30T12:00:00Z"
}
```

Повтор того же ключа с теми же данными возвращает тот же платеж с HTTP 202. Повтор с отличающимся телом возвращает `409 Conflict`. Успешное создание и outbox-запись коммитятся совместно. Изменение порядка ключей в JSON metadata не считается изменением запроса.

```http
GET /api/v1/payments/e58136a6-81a0-4f46-b02c-824fffb605ce
X-API-Key: <api-key>
```

GET возвращает UUID, сумму, валюту, описание, metadata, статус, webhook URL, `created_at` и `processed_at`. Для неизвестного UUID возвращается 404. Эндпоинт `/health` используется контейнерной проверкой здоровья и не входит в публичный платежный API.

## Обработка и надёжность

### Outbox

Запрос платежа и событие `payment.requested` сохраняются в одной транзакции PostgreSQL. Фоновый publisher выбирает неопубликованные записи с `FOR UPDATE SKIP LOCKED`, ставит lease и отправляет постоянные сообщения в `payments.new`. После подтверждения RabbitMQ запись помечается опубликованной. Временная ошибка сбрасывает lease и откладывает повторную публикацию с экспоненциальной задержкой. Если процесс падает после публикации и до отметки в БД, событие может быть отправлено повторно; обработчик идемпотентен.

### Consumer, retry и DLQ

Consumer получает событие через FastStream, блокирует строку платежа и обрабатывает только `pending`. Итоговый статус и запись webhook сохраняются атомарно. Бизнес-результат `failed` является завершённой обработкой и не запускает повторную оплату.

Для временных ошибок обработки используются три попытки всего: первая доставка и две повторные. Повторы направляются в durable очереди `payments.retry.1` и `payments.retry.2`; RabbitMQ возвращает их в `payments.new` через TTL 1 и 2 секунды (экспоненциальная задержка). После третьей неудачи событие направляется в `payments.dlq`. Очереди создаются publisher/subscriber при подключении и первой публикации; сообщения хранят исходный идентификатор платежа и номер попытки. Сообщения переживают рестарт RabbitMQ, а незавершённый обработчик будет доставлен повторно.

### Webhook

После фиксации статуса в отдельной таблице `webhook_deliveries` появляется долговечное уведомление. Отправитель использует ограниченный HTTP-таймаут и повторяет сетевые ошибки и ответы не 2xx с экспоненциальной задержкой (до часа между попытками). Ошибка webhook не возвращает платёж в `pending` и не повторяет платёжную операцию. Доставка как минимум один раз: если удалённая сторона приняла запрос, а процесс завершился до сохранения подтверждения, возможен повтор.

Запрос содержит `X-Webhook-Event-Id` — стабильный UUID события для дедупликации получателем:

```json
{
  "payment_id": "e58136a6-81a0-4f46-b02c-824fffb605ce",
  "status": "succeeded",
  "amount": "1250.00",
  "currency": "RUB",
  "processed_at": "2026-10-30T12:00:04Z"
}
```

## Миграции и тесты

Миграция запускается командой `alembic upgrade head` (в Compose это делает `migrate`). Для локальной разработки:

```powershell
uv pip install --system -e .[test]
$env:PYTHONPATH = "src"
pytest
ruff check src tests migrations
```

Тесты используют SQLite in-memory для API и бизнес-логики; для интеграционной проверки RabbitMQ, блокировок PostgreSQL, TTL/DLX и миграции нужно запустить Compose окружение. Поведение вероятностной обработки в тестах контролируется явным результатом и не зависит от случайности.

## Ограничения

- Платёжный провайдер эмулируется; реальные списания не выполняются.
- Webhook-повторы не ограничены по общему числу попыток, чтобы не терять уведомления из-за длительной недоступности клиента.
- Как и у любой at-least-once системы, возможна повторная доставка на границе подтверждения; получателю webhook следует дедуплицировать запросы по `X-Webhook-Event-Id`.
