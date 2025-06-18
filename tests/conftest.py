import sys
import os
from unittest.mock import patch
import pytest

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
)


@pytest.fixture(autouse=True)
def disable_database_create_all():
    """Мокаем Base.metadata.create_all для предотвращения создания таблиц во время тестов."""
    with patch("src.database.Base.metadata.create_all") as mock_create_all:
        yield
