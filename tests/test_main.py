import pytest
from httpx import AsyncClient
from unittest.mock import AsyncMock, patch
from datetime import date, datetime
from src.app.main import app, get_db
from src.models import SpimexTradingResult


class ScalarResultMock:
    def __init__(self, data):
        self._data = data

    def all(self):
        return self._data


class ExecuteResultMock:
    def __init__(self, data):
        self._data = data

    def scalars(self):
        return ScalarResultMock(self._data)


@pytest.fixture
def mock_db_session():
    session = AsyncMock()
    return session


@pytest.fixture
def client(mock_db_session):
    app.dependency_overrides[get_db] = lambda: mock_db_session

    def _client():
        return AsyncClient(app=app, base_url="http://test")

    yield _client
    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_get_last_trading_dates(client, mock_db_session):
    data = [date(2024, 7, 1), date(2024, 6, 30)]

    async def execute_mock(*args, **kwargs):
        return ExecuteResultMock(data)

    mock_db_session.execute = execute_mock

    async with client() as ac:
        response = await ac.get("/get_last_trading_dates")

    assert response.status_code == 200
    assert response.json() == {"last_trading_dates": ["2024-07-01", "2024-06-30"]}


@pytest.mark.asyncio
async def test_get_dynamics(client, mock_db_session):
    mock_data = [
        SpimexTradingResult(
            id=1,
            exchange_product_id="A001",
            exchange_product_name="Test Oil 1",
            oil_id="A00",
            delivery_basis_id="BAS",
            delivery_basis_name="Basis 1",
            delivery_type_id="T",
            volume=100.0,
            total=10000.0,
            count=10,
            date=date(2024, 1, 1),
            created_on=datetime.now(),
            updated_on=datetime.now(),
        ),
        SpimexTradingResult(
            id=2,
            exchange_product_id="A002",
            exchange_product_name="Test Oil 2",
            oil_id="A00",
            delivery_basis_id="BAS",
            delivery_basis_name="Basis 2",
            delivery_type_id="T",
            volume=200.0,
            total=20000.0,
            count=20,
            date=date(2024, 1, 2),
            created_on=datetime.now(),
            updated_on=datetime.now(),
        ),
    ]

    async def execute_mock(*args, **kwargs):
        return ExecuteResultMock(mock_data)

    mock_db_session.execute = execute_mock

    async with client() as ac:
        response = await ac.get(
            "/get_dynamics?start_date=2024-01-01&end_date=2024-01-02"
        )

    assert response.status_code == 200
    assert len(response.json()) == 2
    assert response.json()[0]["exchange_product_id"] == "A001"
    assert response.json()[1]["exchange_product_id"] == "A002"


@pytest.mark.asyncio
async def test_run_spimex_async_success():
    with patch(
        "src.app.main.process_bulletins_async", new_callable=AsyncMock
    ) as mock_process:
        mock_process.return_value = None
        test_app_client = AsyncClient(app=app, base_url="http://test")
        async with test_app_client as ac:
            response = await ac.post(
                "/run_spimex_async",
                json={
                    "start_date": "2024-01-01",
                    "end_date": "2024-01-02",
                    "output_dir": "test_bulletins",
                },
            )

        assert response.status_code == 200
        assert response.json() == {
            "status": "success",
            "message": "Обработка завершена",
        }
        mock_process.assert_called_once()
