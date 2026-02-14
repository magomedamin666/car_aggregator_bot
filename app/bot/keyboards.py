from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)


def skip_keyboard() -> ReplyKeyboardMarkup:
    """Кнопка 'Пропустить' для пропуска шага фильтрации."""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Пропустить")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def popular_brands_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура с популярными марками автомобилей."""
    brands = [
        "BMW",
        "Mercedes-Benz",
        "Audi",
        "Volkswagen",
        "Toyota",
        "Lada",
        "Hyundai",
        "Kia",
        "Ford",
        "Nissan",
        "Chevrolet",
        "Honda",
        "Mazda",
        "Lexus",
        "Porsche",
    ]
    buttons = [KeyboardButton(text=brand) for brand in brands]
    rows = [buttons[i : i + 3] for i in range(0, len(buttons), 3)]
    rows.append([KeyboardButton(text="Пропустить")])
    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def popular_models_keyboard(brand: str) -> ReplyKeyboardMarkup:
    """Клавиатура с моделями для выбранной марки."""
    models = {
        "BMW": [
            "1 Series",
            "2 Series",
            "3 Series",
            "4 Series",
            "5 Series",
            "6 Series",
            "7 Series",
            "8 Series",
            "X1",
            "X2",
            "X3",
            "X4",
            "X5",
            "X6",
            "X7",
            "Z4",
            "i3",
            "i4",
            "i5",
            "i7",
            "i8",
            "M2",
            "M3",
            "M4",
            "M5",
            "M6",
            "M8",
        ],
        "Mercedes-Benz": [
            "A-Class",
            "B-Class",
            "C-Class",
            "CLA",
            "CLS",
            "E-Class",
            "EQA",
            "EQB",
            "EQC",
            "EQE",
            "EQS",
            "G-Class",
            "GLA",
            "GLB",
            "GLC",
            "GLE",
            "GLS",
            "S-Class",
            "SL",
            "SLC",
            "V-Class",
        ],
        "Audi": [
            "A1",
            "A3",
            "A4",
            "A5",
            "A6",
            "A7",
            "A8",
            "Q2",
            "Q3",
            "Q4",
            "Q5",
            "Q7",
            "Q8",
            "TT",
            "R8",
            "e-tron",
        ],
        "Volkswagen": [
            "Golf",
            "Polo",
            "Passat",
            "Tiguan",
            "Touareg",
            "Arteon",
            "ID.3",
            "ID.4",
            "T-Roc",
            "T-Cross",
        ],
        "Toyota": [
            "Camry",
            "Corolla",
            "RAV4",
            "Land Cruiser",
            "Hilux",
            "Prius",
            "Yaris",
            "C-HR",
            "Supra",
        ],
        "Lada": [
            "Granta",
            "Vesta",
            "Priora",     
            "Kalina", 
            "Niva",
            "XRAY",
            "Largus",
            "Vesta Sport",
            "Granta Drive Active",
        ],
        "Hyundai": [
            "Solaris",
            "Creta",
            "Tucson",
            "Santa Fe",
            "Palisade",
            "Elantra",
            "Sonata",
            "i30",
            "Kona",
        ],
        "Kia": [
            "Rio",
            "Sportage",
            "Sorento",
            "K5",
            "K8",
            "Stinger",
            "Seltos",
            "Carnival",
        ],
        "Ford": ["Focus", "Fiesta", "Kuga", "Explorer", "Mustang", "Transit", "Ranger"],
        "Nissan": [
            "Qashqai",
            "X-Trail",
            "Juke",
            "Murano",
            "Pathfinder",
            "Patrol",
            "GT-R",
        ],
        "Chevrolet": ["Tahoe", "Trailblazer", "Cruze", "Spark", "Malibu", "Camaro"],
        "Honda": ["Civic", "Accord", "CR-V", "HR-V", "Pilot", "Odyssey"],
        "Mazda": ["CX-5", "CX-30", "3", "6", "MX-5", "CX-9"],
        "Lexus": ["RX", "NX", "UX", "ES", "LS", "LX", "GX"],
        "Porsche": ["Cayenne", "Macan", "911", "Panamera", "Taycan", "Boxster"],
    }

    if brand not in models:
        return skip_keyboard()

    buttons = [KeyboardButton(text=model) for model in models[brand]]
    rows = [buttons[i : i + 3] for i in range(0, len(buttons), 3)]
    rows.append([KeyboardButton(text="Пропустить")])
    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def confirm_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура подтверждения создания фильтра."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Сохранить", callback_data="save_filter"),
                InlineKeyboardButton(text="Отменить", callback_data="cancel_filter"),
            ]
        ]
    )