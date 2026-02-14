from aiogram import Router, F
from aiogram.types import (
    Message,
    CallbackQuery,
    KeyboardButton,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardRemove,
)
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext

from app.bot.states import FilterForm
from app.bot.keyboards import (
    skip_keyboard,
    popular_brands_keyboard,
    popular_models_keyboard,
    confirm_keyboard,
)
from app.db.crud import (
    create_filter_set,
    get_active_filters,
    get_user_by_telegram_id,
    create_user,
)
from app.db.session import async_session
from app.db.models import FilterSet
from app.utils.logger import setup_logger
from sqlalchemy import select


logger = setup_logger()
router = Router(name="main_router")


def get_main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✨ Создать фильтр")],
            [KeyboardButton(text="📋 Мои фильтры"), KeyboardButton(text="🗑 Удалить фильтр")],
            [KeyboardButton(text="ℹ️ Помощь")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


@router.message(CommandStart())
async def cmd_start(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username or "Дорогой пользователь"
    logger.info(f"/start от {user_id} ({username})")

    async with async_session() as db:
        try:
            user = await get_user_by_telegram_id(db, user_id)
            if not user:
                user = await create_user(
                    db,
                    telegram_id=user_id,
                    username=username,
                    subscription_status="trial",
                )
                logger.info(f"Создан новый пользователь {user_id}")

            welcome_text = (
                "🚗 <b>Добро пожаловать в CarBot!</b>\n\n"
                "🤖 Я — умный агрегатор объявлений о продаже автомобилей.\n"
                "🔍 Мониторю сайт <b>berkat.ru</b> и присылаю вам новые объявления,\n"
                "которые точно соответствуют вашим критериям.\n\n"
                "✨ <b>Возможности:</b>\n"
                "   • Создавайте фильтры по марке, модели, году и цене\n"
                "   • Получайте уведомления каждые 10 минут о новых объявлениях\n"
                "   • Активируйте/деактивируйте фильтры в один клик\n"
                "   • Никакого спама — только релевантные объявления\n\n"
                "👇 <b>Начните прямо сейчас:</b>\n"
                "   Нажмите кнопку <b>«✨ Создать фильтр»</b> ниже"
            )

            await message.answer(
                welcome_text,
                reply_markup=get_main_menu_keyboard(),
                parse_mode="HTML",
            )

        except Exception as e:
            logger.error(f"Ошибка при /start для {user_id}: {e}")
            await message.answer(
                "❌ Произошла ошибка при запуске бота.\n"
                "Попробуйте позже или напишите разработчику.",
                reply_markup=ReplyKeyboardRemove(),
            )


@router.message(F.text == "ℹ️ Помощь")
async def cmd_help(message: Message):
    help_text = (
        "ℹ️ <b>Справка по CarBot</b>\n\n"
        "🔍 <b>Как это работает:</b>\n"
        "   1. Создайте фильтр с вашими критериями (марка, модель, год, цена)\n"
        "   2. Бот каждые 10 минут проверяет новые объявления на berkat.ru\n"
        "   3. При совпадении — вы получаете уведомление с информацией и ссылкой на объявление\n\n"
        "⚙️ <b>Управление фильтрами:</b>\n"
        "   • «✨ Создать фильтр» — настроить новый фильтр по шагам\n"
        "   • «📋 Мои фильтры» — посмотреть активные фильтры и управлять ими\n"
        "   • «🗑 Удалить фильтр» — удалить фильтр по ID или названию\n\n"
        "💡 <b>Советы:</b>\n"
        "   • Для максимального охвата оставляйте поля «Модель» пустыми\n"
        "   • Фильтр «Lada, цена до 500 000 ₽» найдёт ВАЗ 2107, 2114, Гранту и др.\n"
        "   • Объявления проверяются каждые 10 минут — новые придут быстро!\n\n"
        "🚀 <b>Готовы начать?</b>\n"
        "   Нажмите «✨ Создать фильтр» и настройте свой первый фильтр!"
    )

    await message.answer(
        help_text,
        reply_markup=get_main_menu_keyboard(),
        parse_mode="HTML",
    )


@router.message(F.text == "✨ Создать фильтр")
async def start_new_filter(message: Message, state: FSMContext):
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

    intro_text = (
        "✨ <b>Создание нового фильтра</b>\n\n"
        "Будем настраивать фильтр по шагам.\n"
        "На каждом шаге можно выбрать из списка или ввести своё значение.\n"
        "Чтобы пропустить шаг — нажмите «Пропустить».\n\n"
        "👉 <b>Шаг 1:</b> Выберите марку автомобиля или введите свою:"
    )

    sent = await message.answer(
        intro_text,
        reply_markup=popular_brands_keyboard(),
        parse_mode="HTML",
    )

    await state.update_data(message_ids=[sent.message_id])
    await state.set_state(FilterForm.brand)


@router.message(F.text == "❌ Отмена")
async def cancel_handler(message: Message, state: FSMContext):
    data = await state.get_data()
    message_ids = data.get("message_ids", [])
    message_ids.append(message.message_id)

    for msg_id in message_ids:
        try:
            await message.bot.delete_message(message.chat.id, msg_id)
        except Exception as e:
            logger.debug(f"Не удалось удалить сообщение {msg_id}: {e}")

    await message.answer(
        "🛑 Создание фильтра отменено.",
        reply_markup=get_main_menu_keyboard(),
    )

    await state.clear()


@router.message(FilterForm.brand)
async def process_brand(message: Message, state: FSMContext):
    data = await state.get_data()
    message_ids = data.get("message_ids", [])
    message_ids.append(message.message_id)

    brand = message.text.strip()
    if brand == "Пропустить":
        brand = None
    await state.update_data(brand=brand)

    if brand:
        try:
            models_kb = popular_models_keyboard(brand)
            if models_kb and models_kb != skip_keyboard():
                text = f"👉 <b>Шаг 2:</b> Выберите модель {brand} или введите вручную:"
                sent = await message.answer(text, reply_markup=models_kb, parse_mode="HTML")
            else:
                text = f"👉 <b>Шаг 2:</b> Модель {brand} (например: Vesta, Granta, Priora)\nИли «Пропустить»:"
                sent = await message.answer(text, reply_markup=skip_keyboard(), parse_mode="HTML")
        except Exception as e:
            logger.error(f"Ошибка при генерации клавиатуры моделей для {brand}: {e}")
            text = "👉 <b>Шаг 2:</b> Модель (например: Vesta, Granta)\nИли «Пропустить»:"
            sent = await message.answer(text, reply_markup=skip_keyboard(), parse_mode="HTML")
    else:
        text = "👉 <b>Шаг 2:</b> Модель (например: Vesta, Granta, Priora)\nИли «Пропустить»:"
        sent = await message.answer(text, reply_markup=skip_keyboard(), parse_mode="HTML")

    message_ids.append(sent.message_id)
    await state.update_data(message_ids=message_ids)
    await state.set_state(FilterForm.model)


@router.message(FilterForm.model)
async def process_model(message: Message, state: FSMContext):
    data = await state.get_data()
    message_ids = data.get("message_ids", [])
    message_ids.append(message.message_id)

    model = message.text.strip() if message.text != "Пропустить" else None
    await state.update_data(model=model)

    text = "👉 <b>Шаг 3:</b> Год выпуска ОТ (например: 2018)\nИли «Пропустить»:"
    sent = await message.answer(text, reply_markup=skip_keyboard(), parse_mode="HTML")

    message_ids.append(sent.message_id)
    await state.update_data(message_ids=message_ids)
    await state.set_state(FilterForm.year_from)


@router.message(FilterForm.year_from)
async def process_year_from(message: Message, state: FSMContext):
    data = await state.get_data()
    message_ids = data.get("message_ids", [])
    message_ids.append(message.message_id)

    year_from = None
    if message.text != "Пропустить":
        try:
            year_from = int(message.text.strip())
            if year_from < 1950 or year_from > 2030:
                raise ValueError("Некорректный год")
        except (ValueError, AttributeError):
            sent = await message.answer(
                "❌ Некорректный год. Введите число от 1950 до 2030 или «Пропустить»",
                reply_markup=skip_keyboard(),
            )
            message_ids.append(sent.message_id)
            await state.update_data(message_ids=message_ids)
            return

    await state.update_data(year_from=year_from)
    text = "👉 <b>Шаг 4:</b> Год выпуска ДО (например: 2024)\nИли «Пропустить»:"
    sent = await message.answer(text, reply_markup=skip_keyboard(), parse_mode="HTML")

    message_ids.append(sent.message_id)
    await state.update_data(message_ids=message_ids)
    await state.set_state(FilterForm.year_to)


@router.message(FilterForm.year_to)
async def process_year_to(message: Message, state: FSMContext):
    data = await state.get_data()
    message_ids = data.get("message_ids", [])
    message_ids.append(message.message_id)

    year_to = None
    if message.text != "Пропустить":
        try:
            year_to = int(message.text.strip())
            if year_to < 1950 or year_to > 2030:
                raise ValueError("Некорректный год")
        except (ValueError, AttributeError):
            sent = await message.answer(
                "❌ Некорректный год. Введите число от 1950 до 2030 или «Пропустить»",
                reply_markup=skip_keyboard(),
            )
            message_ids.append(sent.message_id)
            await state.update_data(message_ids=message_ids)
            return

    await state.update_data(year_to=year_to)
    text = "👉 <b>Шаг 5:</b> Цена ОТ (в рублях, например: 300000)\nИли «Пропустить»:"
    sent = await message.answer(text, reply_markup=skip_keyboard(), parse_mode="HTML")

    message_ids.append(sent.message_id)
    await state.update_data(message_ids=message_ids)
    await state.set_state(FilterForm.price_from)


@router.message(FilterForm.price_from)
async def process_price_from(message: Message, state: FSMContext):
    data = await state.get_data()
    message_ids = data.get("message_ids", [])
    message_ids.append(message.message_id)

    price_from = None
    if message.text != "Пропустить":
        try:
            price_text = message.text.strip().replace(" ", "").replace("₽", "").replace("руб", "")
            price_from = int(price_text)
            if price_from < 0 or price_from > 100000000:
                raise ValueError("Некорректная цена")
        except (ValueError, AttributeError):
            sent = await message.answer(
                "❌ Некорректная цена. Введите число от 0 до 100 000 000 или «Пропустить»",
                reply_markup=skip_keyboard(),
            )
            message_ids.append(sent.message_id)
            await state.update_data(message_ids=message_ids)
            return

    await state.update_data(price_from=price_from)
    text = "👉 <b>Шаг 6:</b> Цена ДО (в рублях, например: 1000000)\nИли «Пропустить»:"
    sent = await message.answer(text, reply_markup=skip_keyboard(), parse_mode="HTML")

    message_ids.append(sent.message_id)
    await state.update_data(message_ids=message_ids)
    await state.set_state(FilterForm.price_to)


@router.message(FilterForm.price_to)
async def process_price_to(message: Message, state: FSMContext):
    data = await state.get_data()
    message_ids = data.get("message_ids", [])
    message_ids.append(message.message_id)

    price_to = None
    if message.text != "Пропустить":
        try:
            price_text = message.text.strip().replace(" ", "").replace("₽", "").replace("руб", "")
            price_to = int(price_text)
            if price_to < 0 or price_to > 100000000:
                raise ValueError("Некорректная цена")
        except (ValueError, AttributeError):
            sent = await message.answer(
                "❌ Некорректная цена. Введите число от 0 до 100 000 000 или «Пропустить»",
                reply_markup=skip_keyboard(),
            )
            message_ids.append(sent.message_id)
            await state.update_data(message_ids=message_ids)
            return

    await state.update_data(price_to=price_to)
    text = "👉 <b>Шаг 7:</b> Пробег ДО (в км, например: 100000)\nИли «Пропустить»:"
    sent = await message.answer(text, reply_markup=skip_keyboard(), parse_mode="HTML")

    message_ids.append(sent.message_id)
    await state.update_data(message_ids=message_ids)
    await state.set_state(FilterForm.mileage_to)


@router.message(FilterForm.mileage_to)
async def process_mileage_to(message: Message, state: FSMContext):
    data = await state.get_data()
    message_ids = data.get("message_ids", [])
    message_ids.append(message.message_id)

    mileage_to = None
    if message.text != "Пропустить":
        try:
            mileage_text = message.text.strip().replace(" ", "").replace("км", "").replace("тыс", "")
            if "тыс" in message.text.lower() or (len(mileage_text) <= 4 and int(mileage_text) < 1000):
                mileage_to = int(float(mileage_text) * 1000)
            else:
                mileage_to = int(mileage_text)

            if mileage_to < 0 or mileage_to > 1000000:
                raise ValueError("Некорректный пробег")
        except (ValueError, AttributeError):
            sent = await message.answer(
                "❌ Некорректный пробег. Введите число от 0 до 1 000 000 или «Пропустить»",
                reply_markup=skip_keyboard(),
            )
            message_ids.append(sent.message_id)
            await state.update_data(message_ids=message_ids)
            return

    await state.update_data(mileage_to=mileage_to)
    
    data = await state.get_data()
    
    name_parts = []
    if data.get("brand"):
        name_parts.append(data["brand"])
    if data.get("model"):
        name_parts.append(data["model"])
    if data.get("year_from") or data.get("year_to"):
        yf = data.get("year_from", "")
        yt = data.get("year_to", "")
        if yf and yt:
            name_parts.append(f"{yf}-{yt}")
        elif yf:
            name_parts.append(f"от {yf}")
        elif yt:
            name_parts.append(f"до {yt}")
    if data.get("price_to"):
        price_str = f"{data['price_to']:,}".replace(",", " ")
        name_parts.append(f"до {price_str}₽")
    if data.get("mileage_to"):
        mileage_str = f"{data['mileage_to']:,}".replace(",", " ")
        name_parts.append(f"до {mileage_str}км")
    
    name = " ".join([p for p in name_parts if p]).strip() or "Без названия"
    await state.update_data(name=name)

    text = "✅ <b>Фильтр готов к сохранению</b>\n\n"
    text += f"<b>Название:</b> {name}\n"
    if data.get("brand"):
        text += f"<b>Марка:</b> {data['brand']}\n"
    if data.get("model"):
        text += f"<b>Модель:</b> {data['model']}\n"
    if data.get("year_from"):
        text += f"<b>Год от:</b> {data['year_from']}\n"
    if data.get("year_to"):
        text += f"<b>Год до:</b> {data['year_to']}\n"
    if data.get("price_from"):
        price_str = f"{data['price_from']:,}".replace(",", " ")
        text += f"<b>Цена от:</b> {price_str} ₽\n"
    if data.get("price_to"):
        price_str = f"{data['price_to']:,}".replace(",", " ")
        text += f"<b>Цена до:</b> {price_str} ₽\n"
    if data.get("mileage_to"):
        mileage_str = f"{data['mileage_to']:,}".replace(",", " ")
        text += f"<b>Пробег до:</b> {mileage_str} км\n"

    text += "\n<b>Что дальше?</b>\n"
    text += "• Нажмите ✅ <b>Сохранить</b> — фильтр начнёт работать немедленно\n"
    text += "• Нажмите ❌ <b>Отмена</b> — вернуться в главное меню"

    sent = await message.answer(text, reply_markup=confirm_keyboard(), parse_mode="HTML")
    message_ids.append(sent.message_id)
    await state.update_data(message_ids=message_ids)
    await state.set_state(FilterForm.confirm)


@router.callback_query(F.data == "save_filter")
async def save_filter(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()

    filter_data = {
        "brand": data.get("brand"),
        "model": data.get("model"),
        "min_year": data.get("year_from"),
        "max_year": data.get("year_to"),
        "min_price": data.get("price_from"),
        "max_price": data.get("price_to"),
        "min_mileage": None,
        "max_mileage": data.get("mileage_to"),
        "region": None,
    }

    try:
        async with async_session() as db:
            await create_filter_set(
                db=db,
                user_id=callback.from_user.id,
                name=data["name"],
                filters_json=filter_data,
            )
        logger.info(f"Фильтр '{data['name']}' сохранён для пользователя {callback.from_user.id}")
    except Exception as e:
        logger.error(f"Ошибка сохранения фильтра: {e}")
        try:
            await callback.message.delete()
        except:
            pass
        await callback.message.answer(
            "❌ Ошибка при сохранении фильтра. Попробуйте ещё раз.",
            reply_markup=get_main_menu_keyboard(),
        )
        await state.clear()
        await callback.answer()
        return

    message_ids = data.get("message_ids", [])

    for msg_id in message_ids:
        try:
            await callback.bot.delete_message(callback.message.chat.id, msg_id)
        except Exception as e:
            logger.debug(f"Не удалось удалить сообщение {msg_id}: {e}")

    success_text = (
        "✅ <b>Фильтр успешно сохранён!</b>\n\n"
        f"🔖 <b>Название:</b> {data['name']}\n\n"
        "🔄 <b>Что происходит дальше:</b>\n"
        "   • Бот каждые 10 минут проверяет новые объявления на berkat.ru\n"
        "   • При появлении подходящего объявления — вы получите уведомление\n"
        "   • Уведомления приходят мгновенно, без задержек\n\n"
        "💡 <b>Совет:</b> Вы можете создать несколько фильтров для разных моделей.\n"
        "   Все активные фильтры работают одновременно!"
    )

    await callback.message.answer(
        success_text,
        reply_markup=get_main_menu_keyboard(),
        parse_mode="HTML",
    )

    await state.clear()
    await callback.answer()


@router.callback_query(F.data == "cancel_filter")
async def cancel_filter(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    message_ids = data.get("message_ids", [])

    for msg_id in message_ids:
        try:
            await callback.bot.delete_message(callback.message.chat.id, msg_id)
        except Exception as e:
            logger.debug(f"Не удалось удалить сообщение {msg_id}: {e}")

    await callback.message.answer(
        "🛑 Создание фильтра отменено.",
        reply_markup=get_main_menu_keyboard(),
    )

    await state.clear()
    await callback.answer()


@router.message(F.text == "📋 Мои фильтры")
async def cmd_myfilters(message: Message):
    async with async_session() as db:
        filters = await get_active_filters(db, message.from_user.id)

    if not filters:
        await message.answer(
            "📭 У вас пока нет сохранённых фильтров.\n"
            "Нажмите «✨ Создать фильтр» чтобы начать!",
            reply_markup=get_main_menu_keyboard(),
        )
        return

    text = "📋 <b>Ваши фильтры:</b>\n\n"
    buttons = []
    for f in filters:
        status = "🟢 Активен" if f.is_active else "🔴 Выключен"
        text += f"🆔 <b>ID:</b> {f.id}\n"
        text += f"🏷 <b>Название:</b> {f.name}\n"
        text += f"⚙️ <b>Статус:</b> {status}\n\n"

        row = [
            InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit_filter_{f.id}"),
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"delete_filter_{f.id}"),
        ]
        buttons.append(row)

    kb = InlineKeyboardMarkup(inline_keyboard=buttons)

    await message.answer(
        text,
        reply_markup=kb,
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("delete_filter_"))
async def delete_filter_callback(callback: CallbackQuery):
    filter_id = int(callback.data.split("_")[-1])

    async with async_session() as db:
        result = await db.execute(
            select(FilterSet).where(
                FilterSet.id == filter_id,
                FilterSet.user_id == callback.from_user.id,
            )
        )
        f = result.scalar_one_or_none()

        if not f:
            await callback.answer("❌ Фильтр не найден или это не ваш фильтр.", show_alert=True)
            return

        name = f.name
        await db.delete(f)
        await db.commit()

    try:
        await callback.message.delete()
    except Exception as e:
        logger.debug(f"Не удалось удалить сообщение: {e}")

    await callback.message.answer(
        f"✅ Фильтр «{name}» (ID={filter_id}) успешно удалён!",
        reply_markup=get_main_menu_keyboard(),
    )
    await callback.answer()


@router.message(F.text == "🗑 Удалить фильтр")
async def cmd_deletefilter_button(message: Message):
    text = (
        "🗑 <b>Удаление фильтра</b>\n\n"
        "Введите ID фильтра для удаления.\n"
        "Узнать ID можно в разделе «📋 Мои фильтры».\n\n"
        "Пример: <code>5</code>"
    )
    await message.answer(
        text,
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True,
            one_time_keyboard=True,
        ),
        parse_mode="HTML",
    )


@router.message(F.text.regexp(r"^\d+$"))
async def delete_filter_by_id(message: Message):
    filter_id = int(message.text.strip())

    async with async_session() as db:
        result = await db.execute(
            select(FilterSet).where(
                FilterSet.id == filter_id,
                FilterSet.user_id == message.from_user.id,
            )
        )
        f = result.scalar_one_or_none()

        if not f:
            await message.answer(
                "❌ Фильтр не найден или это не ваш фильтр.",
                reply_markup=get_main_menu_keyboard(),
            )
            return

        name = f.name
        await db.delete(f)
        await db.commit()

    await message.answer(
        f"✅ Фильтр «{name}» (ID={filter_id}) удалён.",
        reply_markup=get_main_menu_keyboard(),
    )