from project.server.main.ina_list import list_urls
from project.server.main.parse import parse_ina
from project.server.main.logger import get_logger

logger = get_logger(__name__)


def create_task_list_urls(args: dict) -> None:
    list_urls(args)




