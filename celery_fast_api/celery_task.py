import time
import logging
from celery import Celery

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

c_app = Celery('celery_task')  # Properly naming the Celery app
c_app.config_from_object('config')

@c_app.task
def mul(a: int, b: int):
    time.sleep(30)  # Simulate a long-running task
    result = a * b
    logger.info('--------task got here--------- processed: %d', result)
    return result
