import hmac
from typing import Annotated

from fastapi import Header, HTTPException, status

from payment_service.config import get_settings


async def require_api_key(x_api_key: Annotated[str | None, Header()] = None) -> None:
    expected = get_settings().api_key.get_secret_value()
    if x_api_key is None or not hmac.compare_digest(x_api_key, expected):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
