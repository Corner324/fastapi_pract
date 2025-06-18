import pytest
from unittest.mock import AsyncMock, patch, MagicMock, mock_open
from datetime import date, datetime
import pandas as pd
from src.spimex_async import parse_page_links, parse_bulletin, process_bulletins_async
from bs4 import BeautifulSoup


@pytest.fixture
def mock_excel_data():
    """Фикстура, имитирующая структуру Excel-файла."""
    data = [[None] * 7 for _ in range(6)]
    headers = [
        None,
        "Код Инструмента",
        "Наименование Инструмента",
        "Базис поставки",
        "Объем Договоров в единицах измерения",
        "Обьем Договоров, руб.",
        "Количество Договоров, шт.",
    ]
    data.append(headers)
    data.append([None] * 7)
    data.append([None, "A001-B1-T", "Нефть А", "БАЗИС 1", 100.0, 10000.0, 10])
    data.append([None, "B002-B2-T", "Нефть Б", "БАЗИС 2", 200.0, 20000.0, 20])
    return pd.DataFrame(data)


@pytest.fixture
def mock_file_system():
    """Фикстура для мока файловой системы."""
    with patch("os.makedirs") as mock_makedirs, patch(
        "os.path.exists", return_value=False
    ) as mock_exists, patch(
        "os.path.join", side_effect=lambda *args: "/".join(str(a) for a in args)
    ) as mock_join, patch(
        "src.spimex_async.open", mock_open()
    ) as m_open_async, patch(
        "src.spimex_sync.open", mock_open()
    ) as m_open_sync:
        yield mock_makedirs, mock_exists, mock_join, m_open_async, m_open_sync


@pytest.fixture
def mock_async_session():
    """Фикстура для асинхронной сессии базы данных."""
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock()
    mock_session.commit = AsyncMock()
    mock_session.rollback = AsyncMock()
    mock_session.__aenter__.return_value = mock_session
    mock_session.__aexit__.return_value = None
    return mock_session


@pytest.fixture
def mock_sync_session():
    """Фикстура для синхронной сессии базы данных."""
    mock_session = MagicMock()
    mock_session.execute = MagicMock()
    mock_session.commit = MagicMock()
    mock_session.rollback = MagicMock()
    mock_session.__enter__.return_value = mock_session
    mock_session.__exit__.return_value = None
    return mock_session


@pytest.fixture
def mock_db_session(mock_async_session, mock_sync_session):
    """Фикстура для мока сессий базы данных."""
    with patch(
        "src.spimex_async.async_sessionmaker", return_value=lambda: mock_async_session
    ), patch("src.spimex_sync.SyncSession", return_value=mock_sync_session), patch(
        "sqlalchemy.dialects.postgresql.insert", return_value=MagicMock()
    ):
        yield mock_async_session, mock_sync_session


@pytest.mark.asyncio
async def test_parse_page_links():
    """Тест функции parse_page_links."""
    soup = BeautifulSoup(
        """
        <a class="accordeon-inner__item-title link xls" href="/upload/reports/oil_xls/oil_xls_20240101_test.xls">link1</a>
        <a class="accordeon-inner__item-title link xls" href="/upload/reports/oil_xls/oil_xls_20231231_test.xls">link2</a>
        <a class="accordeon-inner__item-title link xls" href="/upload/reports/not_xls_file.doc">link3</a>
        <a class="accordeon-inner__item-title link xls" href="/upload/reports/oil_xls_bad_date.xls">link4</a>
        """,
        "html.parser",
    )
    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 1)
    base_url = "https://spimex.com"
    urls = parse_page_links(soup, start_date, end_date, base_url)
    assert len(urls) == 1
    assert urls[0][0].endswith("20240101_test.xls")
    assert urls[0][1] == date(2024, 1, 1)


def test_parse_bulletin(mock_excel_data):
    """Тест функции parse_bulletin."""
    with patch("pandas.read_excel", return_value=mock_excel_data):
        with patch("src.spimex_async.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 7, 1, 10, 0, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            file_path = "test.xls"
            trade_date = date(2024, 1, 1)
            results = parse_bulletin(file_path, trade_date)
            assert len(results) == 2
            assert results[0]["exchange_product_id"] == "A001-B1-T"
            assert results[1]["exchange_product_id"] == "B002-B2-T"
            assert results[0]["date"] == trade_date
            assert results[1]["date"] == trade_date
            assert results[0]["count"] == 10
            assert results[1]["count"] == 20


@pytest.mark.asyncio
async def test_process_bulletins_async(
    mock_file_system, mock_db_session, mock_excel_data
):
    """Тест асинхронной обработки бюллетеней."""
    mock_makedirs, mock_exists, mock_join, m_open_async, m_open_sync = mock_file_system
    mock_async_session, _ = mock_db_session
    bulletin_urls = [
        (
            "https://spimex.com/upload/reports/oil_xls/oil_xls_20240101_test.xls",
            date(2024, 1, 1),
        ),
        (
            "https://spimex.com/upload/reports/oil_xls/oil_xls_20240102_test.xls",
            date(2024, 1, 2),
        ),
    ]
    with patch("src.spimex_async.get_bulletin_urls", return_value=bulletin_urls), patch(
        "src.spimex_async.download_bulletin", new_callable=AsyncMock, return_value=True
    ), patch("pandas.read_excel", return_value=mock_excel_data), patch(
        "src.spimex_async.datetime"
    ) as mock_dt:
        mock_dt.now.return_value = datetime(2024, 7, 1, 10, 0, 0)
        mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 2)
        output_dir = "temp_bulletins"
        await process_bulletins_async(start_date, end_date, output_dir)
        mock_makedirs.assert_called_once_with(output_dir, exist_ok=True)
        assert m_open_async.call_count == 0
        assert m_open_sync.call_count == 0
        assert mock_async_session.execute.call_count >= 1
