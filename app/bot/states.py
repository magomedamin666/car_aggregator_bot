from aiogram.fsm.state import State, StatesGroup


class FilterForm(StatesGroup):
    """Состояния для создания нового фильтра."""
    brand = State()
    model = State()
    year_from = State()
    year_to = State()
    price_from = State()
    price_to = State()
    mileage_to = State()
    region = State()
    confirm = State()