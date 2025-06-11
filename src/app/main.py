from fastapi import FastAPI, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from ..database import get_db, sync_engine, Base
from ..models import SpimexTradingResult
from sqlalchemy import select
from datetime import date
from typing import Optional


app = FastAPI()


# Создание таблиц базы данных при запуске приложения
@app.on_event("startup")
def startup_event():
    Base.metadata.create_all(bind=sync_engine)


@app.get("/get_last_trading_dates")
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


@app.get("/get_dynamics")
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
    return {"dynamics": dynamics}


@app.get("/get_trading_results")
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
    return {"trading_results": trading_results}
