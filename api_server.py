# api_server.py
import uvicorn
import logging
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import select
from typing import Optional, List
from pydantic import BaseModel, ConfigDict

from app.core.config import settings
from app.db.models import Ad

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Car Aggregator API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logger.info(f"🔗 Подключение к БД: {settings.DB_URL}")
engine = create_async_engine(settings.DB_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)


class AdResponse(BaseModel):
    id: int
    title: str
    price: Optional[int]
    brand: Optional[str]
    model: Optional[str]
    year: Optional[int]
    mileage: Optional[int]
    region: Optional[str]
    url: str
    photo_url: Optional[str]
    parsed_at: Optional[datetime]  # ✅ ИСПРАВЛЕНО: datetime вместо str
    
    model_config = ConfigDict(from_attributes=True)


@app.get("/ads", response_model=List[AdResponse])
async def get_ads(
    brand: Optional[str] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    limit: int = 50
):
    logger.info(f"📥 GET /ads: brand={brand}, limit={limit}")
    
    try:
        async with async_session() as session:
            query = select(Ad).where(Ad.source == "berkat.ru")
            
            if brand:
                query = query.where(Ad.brand.ilike(f"%{brand}%"))
            if min_price is not None:
                query = query.where(Ad.price >= min_price)
            if max_price is not None:
                query = query.where(Ad.price <= max_price)
            
            query = query.order_by(Ad.parsed_at.desc()).limit(limit)
            
            result = await session.execute(query)
            ads = result.scalars().all()
            
            logger.info(f"✅ Возвращаем {len(ads)} объявлений")
            return ads
            
    except Exception as e:
        logger.error(f"❌ Ошибка в /ads: {type(e).__name__}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")


@app.get("/health")
async def health():
    try:
        async with async_session() as session:
            await session.execute(select(Ad).limit(1))
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        logger.error(f"❌ Health check failed: {e}")
        return {"status": "error", "detail": str(e)}, 500


if __name__ == "__main__":
    logger.info("🚀 Запуск API сервера на порту 8000")
    uvicorn.run("api_server:app", host="0.0.0.0", port=8000, reload=False)