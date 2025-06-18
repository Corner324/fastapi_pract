from fastapi import FastAPI, Depends, Query, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from database import get_db, sync_engine, Base
from models import SpimexTradingResult
from sqlalchemy import select
from datetime import date
from typing import Optional, List
from cache import cache_response, get_redis_client, clear_cache, schedule_cache_reset
import asyncio
from spimex_async import process_bulletins_async
import os
from datetime import datetime, timedelta
from trading_result_schema import TradingResultModel


app = FastAPI()

cache_reset_task = None


@app.on_event("startup")
async def startup_event():
    global cache_reset_task
    Base.metadata.create_all(bind=sync_engine)
    # Проверка подключения к Redis при запуске
    try:
        redis_client = await get_redis_client()
        if redis_client is None:
            print(
                "Предупреждение: Redis недоступен. API будет работать без кэширования."
            )
        else:
            print("Успешное подключение к Redis.")
            cache_reset_task = asyncio.create_task(schedule_cache_reset())
            print("Запланирован ежедневный сброс кэша в 14:11")
    except Exception as e:
        print(f"Не удалось подключиться к Redis при запуске: {e}")
        print("API будет работать без кэширования.")


@app.on_event("shutdown")
async def shutdown_event():
    global cache_reset_task
    if cache_reset_task:
        cache_reset_task.cancel()
        try:
            await cache_reset_task
        except asyncio.CancelledError:
            pass


@app.get("/get_last_trading_dates")
@cache_response(key_prefix="last_trading_dates")
async def get_last_trading_dates(
    db: AsyncSession = Depends(get_db),
    count: Optional[int] = Query(10, description="Количество последних торговых дней"),
):
    """Список дат последних торговых дней (фильтрация по кол-ву последних торговых дней)."""
    query = (
        select(SpimexTradingResult.date)
        .distinct()
        .order_by(SpimexTradingResult.date.desc())
    )
    if count is not None:
        query = query.limit(count)
    result = await db.execute(query)
    last_trading_dates = [d.strftime("%Y-%m-%d") for d in result.scalars().all()]
    return {"last_trading_dates": last_trading_dates}


@app.get("/get_dynamics", response_model=List[TradingResultModel])
async def get_dynamics(
    db: AsyncSession = Depends(get_db),
    start_date: date = Query(..., description="Дата начала периода (YYYY-MM-DD)"),
    end_date: date = Query(..., description="Дата окончания периода (YYYY-MM-DD)"),
    oil_id: Optional[str] = Query(None, description="Идентификатор нефти"),
    delivery_type_id: Optional[str] = Query(
        None, description="Идентификатор типа поставки"
    ),
    delivery_basis_id: Optional[str] = Query(
        None, description="Идентификатор базиса поставки"
    ),
):
    """Список торгов за заданный период (фильтрация по oil_id, delivery_type_id, delivery_basis_id, start_date, end_date)."""
    query = select(SpimexTradingResult).filter(
        SpimexTradingResult.date >= start_date, SpimexTradingResult.date <= end_date
    )

    if oil_id:
        query = query.filter(SpimexTradingResult.oil_id == oil_id)
    if delivery_type_id:
        query = query.filter(SpimexTradingResult.delivery_type_id == delivery_type_id)
    if delivery_basis_id:
        query = query.filter(SpimexTradingResult.delivery_basis_id == delivery_basis_id)

    result = await db.execute(query.order_by(SpimexTradingResult.date))
    dynamics = result.scalars().all()
    return dynamics


@app.get("/get_trading_results", response_model=List[TradingResultModel])
async def get_trading_results(
    db: AsyncSession = Depends(get_db),
    oil_id: Optional[str] = Query(None, description="Идентификатор нефти"),
    delivery_type_id: Optional[str] = Query(
        None, description="Идентификатор типа поставки"
    ),
    delivery_basis_id: Optional[str] = Query(
        None, description="Идентификатор базиса поставки"
    ),
    limit: Optional[int] = Query(
        100, description="Максимальное количество результатов"
    ),
):
    """Список последних торгов (фильтрация по oil_id, delivery_type_id, delivery_basis_id)."""
    query = select(SpimexTradingResult).order_by(SpimexTradingResult.date.desc())

    if oil_id:
        query = query.filter(SpimexTradingResult.oil_id == oil_id)
    if delivery_type_id:
        query = query.filter(SpimexTradingResult.delivery_type_id == delivery_type_id)
    if delivery_basis_id:
        query = query.filter(SpimexTradingResult.delivery_basis_id == delivery_basis_id)

    if limit is not None:
        query = query.limit(limit)

    result = await db.execute(query)
    trading_results = result.scalars().all()
    return trading_results


@app.post("/clear_cache")
async def clear_api_cache(pattern: str = "*"):
    """Очищает кеш API по заданному шаблону."""
    success = await clear_cache(pattern)
    if success:
        return {
            "status": "success",
            "message": f"Кеш по шаблону '{pattern}' успешно очищен",
        }
    else:
        return {"status": "error", "message": "Не удалось очистить кеш"}


@app.post("/run_spimex_async")
async def run_spimex_async(
    start_date: str = "2024-01-01",
    end_date: str = "2024-01-31",
    output_dir: str = "bulletins",
):
    """Запускает асинхронную обработку бюллетеней."""
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        await process_bulletins_async(start_dt, end_dt, output_dir)
        return {"status": "success", "message": "Обработка завершена"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
