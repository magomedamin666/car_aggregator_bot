# api/main.py
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List
from pydantic import BaseModel

from app.db.session import async_session
from app.db.crud import get_ads_by_filters, create_user_filter
from app.db.models import Ad

app = FastAPI(title="Car Aggregator API")

# CORS для Expo Go
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В продакшене укажите конкретные домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Модели для запросов/ответов
class FilterCreate(BaseModel):
    user_id: str  # Уникальный идентификатор пользователя (можно генерировать на клиенте)
    brand: Optional[str] = None
    model: Optional[str] = None
    min_price: Optional[int] = None
    max_price: Optional[int] = None
    min_year: Optional[int] = None
    max_year: Optional[int] = None
    max_mileage: Optional[int] = None
    region: Optional[str] = None

class AdResponse(BaseModel):
    id: int
    title: str
    price: Optional[int]
    brand: str
    model: str
    year: Optional[int]
    mileage: Optional[int]
    region: str
    url: str
    photo_url: Optional[str]
    
    class Config:
        from_attributes = True

@app.get("/ads", response_model=List[AdResponse])
async def get_ads(
    brand: Optional[str] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    limit: int = 50,
    db: AsyncSession = Depends(lambda: async_session())
):
    """Получить список авто с фильтрами"""
    # Здесь нужно адаптировать ваш CRUD-метод под запросы из мобильного приложения
    ads = await get_ads_by_filters(db, brand=brand, min_price=min_price, max_price=max_price, limit=limit)
    return ads

@app.post("/filters")
async def create_filter(filter_data: FilterCreate, db: AsyncSession = Depends(lambda: async_session())):
    """Создать фильтр для пользователя"""
    new_filter = await create_user_filter(db, filter_data.dict())
    return {"status": "ok", "filter_id": new_filter.id}

@app.get("/health")
async def health_check():
    return {"status": "running", "service": "car-aggregator-api"}

# Запуск: uvicorn api.main:app --host 0.0.0.0 --port 8000