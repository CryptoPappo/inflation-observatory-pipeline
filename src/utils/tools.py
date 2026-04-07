from requests import Session, Response
from requests.exceptions import HTTPError
from tenacity import (
        retry,
        retry_if_not_exception_type,
        wait_exponential,
        stop_after_attempt
)

from src.utils.logging import get_logger
logger = get_logger("tools")

def log_after_retry(retry_state):
    logger.error(
            f"Retrying {retry_state.fn.__name__}:"
            f"Attempt {retry_state.attempt_number} failed with {retry_state.outcome.exception()}"
    )

@retry(
    retry=retry_if_not_exception_type(HTTPError),
    wait=wait_exponential(min=1, max=60),
    stop=stop_after_attempt(5),
    after=log_after_retry
)
def safe_get(session: Session, url: str, **kwargs) -> Response:
    response = session.get(url, **kwargs)
    response.raise_for_status()
    
    return response
