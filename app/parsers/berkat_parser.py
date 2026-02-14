import asyncio
import logging
import os
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional

import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from app.bot.telegram_bot import send_ad_notification
from app.db.crud import (
    get_ad_by_source_external,
    get_all_active_filters,
    has_sent_notification,
    mark_notification_sent,
)
from app.db.models import Ad, FilterSet
from app.db.session import async_session


os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/berkat_parser.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


BASE_URL = "https://berkat.ru"
SEARCH_URL = "https://berkat.ru/avto"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": f"{BASE_URL}/",
}


async def fetch(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    try:
        async with session.get(url, headers=HEADERS, timeout=15) as resp:
            if resp.status == 200:
                logger.info(f"Успешно загружена страница: {url}")
                return await resp.text()
            logger.warning(f"[{resp.status}] {url}")
            return None
    except asyncio.TimeoutError:
        logger.error(f"Таймаут при загрузке {url}")
        return None
    except Exception as e:
        logger.error(f"Ошибка загрузки {url}: {e}")
        return None


def matches_filter(ad: Dict, filter_set: FilterSet) -> bool:
    try:
        filters = filter_set.filters_json
        ad_title = ad.get("title", "")[:40]
        filter_name = filter_set.name

        logger.info(f"Проверка: '{ad_title}...' vs фильтр '{filter_name}'")

        ad_parsed_at = ad.get("parsed_at")
        filter_created_at = filter_set.created_at

        if ad_parsed_at and filter_created_at:
            filter_created_naive = (
                filter_created_at.replace(tzinfo=None)
                if filter_created_at.tzinfo
                else filter_created_at
            )
            if ad_parsed_at < filter_created_naive:
                logger.debug("Пропускаем: объявление старше фильтра")
                return False

        brand = filters.get("brand")
        if brand:
            ad_brand = ad.get("brand", "").lower().strip()
            filter_brand = brand.lower().strip()

            BRAND_SYNONYMS = {
                "lada": [
                    "lada", "лада", "ваз", "ваза", "вазик", "жигули", "жигуль",
                    "классика", "копейка", "шестерка", "семерка", "восьмерка",
                    "девятка", "десятка", "приора", "приору", "гранта", "гранту",
                    "калина", "калину", "веста", "весту",
                ],
                "renault": ["renault", "рено", "реноль", "ренуо", "ренаулт"],
                "kia": ["kia", "киа", "кья", "киас", "киашка"],
                "hyundai": ["hyundai", "хендай", "хюндай", "хендэ"],
                "nissan": ["nissan", "ниссан", "нисан"],
                "toyota": ["toyota", "тойота", "тоета"],
                "mazda": ["mazda", "мазда", "мазды"],
                "volkswagen": ["volkswagen", "фольксваген", "ваген", "жук"],
                "skoda": ["skoda", "шкода", "шкодовский"],
                "ford": ["ford", "форд", "форды"],
                "chevrolet": ["chevrolet", "шевроле", "шевроль"],
                "bmw": ["bmw", "бмв", "бэха", "беха", "беху"],
                "mercedes": ["mercedes", "мерседес", "мерс"],
                "audi": ["audi", "ауди", "аудик"],
                "volvo": ["volvo", "вольво", "волво"],
                "subaru": ["subaru", "субару", "субарус"],
                "honda": ["honda", "хонда", "хондуля"],
                "suzuki": ["suzuki", "сузуки", "сузукис"],
                "mitsubishi": ["mitsubishi", "мицубиси", "мицубиша"],
                "opel": ["opel", "опель", "опелек"],
                "daewoo": ["daewoo", "дэу", "даеву"],
                "gaz": ["gaz", "газ", "газель", "газик"],
                "uaz": ["uaz", "уаз", "уазик", "буханка"],
                "moskvich": ["moskvich", "москвич", "москвичи"],
            }

            ad_brand_clean = "".join(c for c in ad_brand if c.isalnum() or c in " -_")
            matched_brand = False

            for canonical, synonyms in BRAND_SYNONYMS.items():
                if filter_brand in synonyms or filter_brand == canonical:
                    for syn in synonyms:
                        if syn in ad_brand_clean or ad_brand_clean in syn:
                            matched_brand = True
                            break
                    if matched_brand:
                        break

            if not matched_brand:
                return False

        model = filters.get("model")
        if model:
            ad_model = ad.get("model", "").lower().strip()
            filter_model = model.lower().strip()
            if filter_model not in ad_model:
                return False

        ad_year = ad.get("year")
        if ad_year is not None:
            min_year = filters.get("min_year")
            max_year = filters.get("max_year")
            if min_year and ad_year < min_year:
                return False
            if max_year and ad_year > max_year:
                return False

        ad_price = ad.get("price")
        if ad_price is not None:
            min_price = filters.get("min_price")
            max_price = filters.get("max_price")
            if min_price and ad_price < min_price:
                return False
            if max_price and ad_price > max_price:
                return False

        ad_mileage = ad.get("mileage")
        if ad_mileage is not None:
            max_mileage = filters.get("max_mileage")
            if max_mileage and ad_mileage > max_mileage:
                return False

        region = filters.get("region")
        if region:
            ad_region = ad.get("region", "").lower().strip()
            filter_region = region.lower().strip()

            CITY_TO_REGION = {
                "назрань": "ингушетия",
                "магас": "ингушетия",
                "карабулак": "ингушетия",
                "грозный": "чечня",
                "шали": "чечня",
                "махачкала": "дагестан",
                "дербент": "дагестан",
                "москва": "москва",
                "мск": "москва",
                "санкт-петербург": "санкт-петербург",
                "спб": "санкт-петербург",
                "питер": "санкт-петербург",
            }

            ad_region_norm = CITY_TO_REGION.get(ad_region, ad_region)
            if filter_region not in ad_region_norm and ad_region_norm not in filter_region:
                return False

        return True

    except Exception as e:
        logger.error(f"Ошибка в матчинге: {e}", exc_info=True)
        return False


def parse_ad_block(block) -> Optional[Dict]:
    try:
        link_tag = block.find("h3", class_="board_list_item_title")
        if link_tag:
            link_tag = link_tag.find("a", href=True)

        if not link_tag or not link_tag.get("href"):
            link_tag = block.find("a", href=lambda href: href and "/content/" in href)

        if not link_tag or not link_tag.get("href"):
            link_tag = block.find("a", href=True)

        if not link_tag or not link_tag.get("href"):
            return None

        href = link_tag["href"].strip()
        url = urljoin(BASE_URL, href)

        external_id = url.rstrip("/").split("/")[-1].strip()
        if not external_id.isdigit() or len(external_id) < 4:
            external_id = str(hash(url))[:20]

        title_tag = block.find("h3", class_="board_list_item_title") or block.find("a")
        title = title_tag.get_text(strip=True) if title_tag else ""

        title_lower = title.lower()
        non_car_keywords = [
            "эвакуатор", "установка гбо", "ремонт", "покраска", "диагностика",
            "шиномонтаж", "запчасти", "детали", "аренда авто", "прокат авто",
            "грузовой", "грузовик", "камаз", "автобус", "прицеп", "мотоцикл",
            "скутер", "квадроцикл", "выкуп авто", "залог", "на запчасти",
            "битый", "аварийный",
        ]
        if any(word in title_lower for word in non_car_keywords):
            return None

        known_brands = [
            "lada", "ваз", "лада", "приора", "приору", "гранта", "гранту",
            "калина", "калину", "веста", "весту", "ренуо", "renault", "рено",
            "киа", "kia", "хендай", "hyundai", "тойота", "toyota", "ниссан",
            "nissan", "мазда", "mazda", "мицубиси", "mitsubishi", "шкода",
            "skoda", "фольксваген", "волкцваген", "volkswagen", "vw", "опель",
            "opel", "форд", "ford", "шевроле", "шевролет", "chevrolet", "мерседес",
            "мерс", "бмв", "бэха", "беха", "беху", "bmw", "ауди", "audi", "вольво",
            "volvo", "субару", "subaru", "хонда", "хунда", "хонду", "honda",
            "сузуки", "suzuki", "дэу", "даеву", "daewoo", "газель", "газ", "уаз",
            "уазик", "moskvich", "москвич", "элантра", "elantra", "solaris",
            "соларис", "рио", "rio", "creta", "крета",
        ]

        brand = ""
        model = ""
        for brand_candidate in known_brands:
            if brand_candidate in title_lower:
                brand = brand_candidate.capitalize()
                parts = title.split(brand_candidate, 1)
                if len(parts) > 1:
                    model = parts[1].strip()
                break

        if not brand:
            return None

        year_match = re.search(r"\b(19[89]\d|20[012]\d)\b", title)
        year = int(year_match.group(1)) if year_match else None

        price = None
        price_tag = block.find(string=re.compile(r"₽|руб|тыс", re.I))
        if price_tag:
            price_str = price_tag.find_parent().get_text(strip=True)
            price_match = re.search(r"(\d[\d\s]*)", price_str.replace("\xa0", " "))
            if price_match:
                price_text = price_match.group(1).replace(" ", "").replace("\xa0", "")
                try:
                    price = int(price_text)
                    if 10 <= price <= 5000:
                        price *= 1000
                    if price < 5000 or price > 50000000:
                        price = None
                except (ValueError, OverflowError):
                    price = None

        mileage = None
        mileage_text = block.find(string=re.compile(r"пробег|км|тыс", re.I))
        if mileage_text:
            parent_text = mileage_text.find_parent().get_text(strip=True)
            mileage_match = re.search(
                r"(\d+[\s\.]?\d*)\s*(тыс\.?|т\.?|км)", parent_text, re.I
            )
            if mileage_match:
                try:
                    mileage_val = float(
                        mileage_match.group(1).replace(" ", "").replace(".", "")
                    )
                    if (
                        "тыс" in mileage_match.group(2).lower()
                        or "т" in mileage_match.group(2).lower()
                    ):
                        mileage = int(mileage_val * 1000)
                    else:
                        mileage = int(mileage_val)
                    if mileage < 1000 or mileage > 1000000:
                        mileage = None
                except (ValueError, TypeError, OverflowError):
                    mileage = None

        region = ""
        region_tag = block.find(
            string=re.compile(
                r"Москва|СПб|Санкт-Петербург|Новосибирск|Екатеринбург|Казань|Нижний|Челябинск|"
                r"Омск|Самара|Ростов|Уфа|Красноярск|Воронеж|Пермь|Волгоград|Назрань|Магас|"
                r"Ингушетия|Чечня|Дагестан|Грозный|Дербент|Махачкала|Владикавказ",
                re.I,
            )
        )
        if region_tag:
            region = region_tag.find_parent().get_text(strip=True)[:50]

        img_tag = block.find("img", src=True)
        if img_tag:
            src = img_tag["src"].strip()
            photo_url = urljoin(BASE_URL, src) if src else None
        else:
            photo_url = None

        return {
            "source": "berkat.ru",
            "external_id": external_id,
            "title": title,
            "price": price,
            "brand": brand,
            "model": model[:100],
            "year": year,
            "mileage": mileage,
            "region": region,
            "url": url,
            "photo_url": photo_url,
            "parsed_at": datetime.now(timezone.utc).replace(tzinfo=None),
        }

    except Exception as e:
        logger.error(f"Ошибка парсинга блока: {e}", exc_info=True)
        return None


async def parse_berkat_pages(
    session: aiohttp.ClientSession, max_pages: int = 5
) -> List[Dict]:
    all_ads = []
    logger.info(f"Начало парсинга {max_pages} страниц berkat.ru")

    for page in range(1, max_pages + 1):
        url = f"{SEARCH_URL}?page={page}" if page > 1 else SEARCH_URL
        html = await fetch(session, url)
        if not html:
            logger.warning(f"Страница {page} не загружена")
            continue

        soup = BeautifulSoup(html, "lxml")
        ad_blocks = soup.select("div.board_list_item")

        if not ad_blocks:
            logger.warning(f"Страница {page}: карточки не найдены.")
            continue

        page_ads = []
        for block in ad_blocks:
            ad_data = parse_ad_block(block)
            if ad_data:
                page_ads.append(ad_data)

        logger.info(
            f"Страница {page}: найдено {len(ad_blocks)} блоков, "
            f"спарсено {len(page_ads)} авто-объявлений"
        )
        all_ads.extend(page_ads)

    logger.info(f"Всего спарсено объявлений: {len(all_ads)}")
    return all_ads


async def save_new_ads(ads: List[Dict]) -> List[Ad]:
    saved_ads = []
    saved_count = 0

    async with async_session() as db:
        for ad_data in ads:
            try:
                existing = await get_ad_by_source_external(
                    db, ad_data["source"], ad_data["external_id"]
                )
                if not existing:
                    ad = Ad(**ad_data)
                    db.add(ad)
                    saved_ads.append(ad)
                    saved_count += 1
            except Exception as e:
                logger.error(
                    f"Ошибка сохранения объявления '{ad_data.get('title', 'N/A')}': {e}"
                )

        if saved_ads:
            await db.commit()
            for ad in saved_ads:
                await db.refresh(ad)

        logger.info(f"Сохранено новых объявлений: {saved_count}")

    return saved_ads


async def check_filters_and_notify(saved_ads: List[Ad]) -> None:
    if not saved_ads:
        logger.info("Нет новых объявлений для проверки фильтров")
        return

    logger.info(f"Проверка {len(saved_ads)} новых объявлений по фильтрам пользователей...")

    async with async_session() as db:
        active_filters = await get_all_active_filters(db)
        logger.info(f"Найдено активных фильтров: {len(active_filters)}")

        for ad in saved_ads:
            for filter_set in active_filters:
                if matches_filter(ad.__dict__, filter_set):
                    if await has_sent_notification(db, filter_set.user_id, ad.id, filter_set.id):
                        logger.debug(
                            f"Уведомление пропущено (уже отправлялось): "
                            f"пользователь={filter_set.user_id}, объявление={ad.id}"
                        )
                        continue

                    try:
                        await send_ad_notification(
                            telegram_id=filter_set.user_id,
                            ad=ad,
                            filter_name=filter_set.name,
                        )
                        await mark_notification_sent(db, filter_set.user_id, ad.id, filter_set.id)
                        logger.info(
                            f"Уведомление отправлено пользователю {filter_set.user_id} "
                            f"по фильтру '{filter_set.name}'"
                        )
                    except Exception as e:
                        logger.error(f"Ошибка отправки уведомления: {e}")


async def berkat_parse_task_async() -> None:
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info(f"[{start_time}] Запуск парсинга berkat.ru...")

    try:
        async with aiohttp.ClientSession() as session:
            ads = await parse_berkat_pages(session, max_pages=5)
            if ads:
                saved_ads = await save_new_ads(ads)
                if saved_ads:
                    await check_filters_and_notify(saved_ads)
                else:
                    logger.info("Новые объявления не найдены.")
            else:
                logger.warning("Авто-объявления не найдены.")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"[{end_time}] Парсинг завершён. Время выполнения: {duration:.2f} сек")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Критическая ошибка при парсинге: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    logger.info("Ручной запуск парсера berkat.ru")
    asyncio.run(berkat_parse_task_async())