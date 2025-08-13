""" Русский """

language = 'ru'

novalue = 'нет значения'

time_variables = {
    'months': ['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн', 'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек'],
    'century': 'век',
    'millennium': 'тысячелетие',
    'decade': '-е годы',
    'AD': 'н. э.',
    'BC': 'до н. э.',
    'years': 'лет',
    'ten thousand years': 'десятков тысяч лет',
    'hundred thousand years': 'сотен тысяч лет',
    'million years': 'миллионов лет',
    'tens of millions of years': 'десятков миллионов лет',
    'hundred million years': 'сотен миллионов лет',
    'billion years': 'миллиардов лет',
}

def merge_entity_text(label, description, aliases, properties):
    """
    Объединяет атрибуты сущности (label, description, aliases, properties) в одну
    осмысленную строку естественного языка.

    Параметры:
    - label: строка — название сущности.
    - description: строка — краткое описание.
    - aliases: список псевдонимов.
    - instanceof: список классов, к которым принадлежит сущность.
    - properties: словарь свойств.

    Возвращает:
    - Строку, содержащую название, описание, псевдонимы (если есть) и перечень
      свойств. Если свойств нет, строка заканчивается точкой.
    """
    text = label

    if len(description) > 0:
        text += f", {description}"

    if len(aliases) > 0:
        text += f", также известный как {', '.join(aliases)}"

    if len(properties) > 0:
        properties_text = properties_to_text(properties)
        text = f"{text}. Атрибуты: {properties_text}"
    else:
        text = f"{text}."

    return text

def qualifiers_to_text(qualifiers):
    """
    Преобразует список квалификаторов в удобочитаемую строку.

    Параметры:
    - qualifiers: список словарей с ключами 'property_label' и 'values'.

    Возвращает:
    - Строку с перечислением квалификаторов.
    """
    text = ""
    for claim in qualifiers:
        property_label = claim['property_label']
        qualifier_values = claim['values']
        if qualifier_values:
            if text:
                text += " "
            text += f"({property_label}: {', '.join(qualifier_values)})"
        else:
            if len(text) > 0:
                text += f" "
            text += f"(есть {property_label})"

    return f" {text}" if text else ""

def properties_to_text(properties):
    """
    Преобразует словарь свойств (claims) в удобочитаемую строку.

    Параметры:
    - properties: список словарей, каждый содержит 'property_label' и 'values'.

    Возвращает:
    - Строку со свойствами и их значениями.
    """
    properties_text = ""
    for claim in properties:
        property_label = claim['property_label']
        claim_values = claim['values']

        if claim_values:
            claims_text = ""
            for claim_value in claim_values:
                if claims_text:
                    claims_text += ", "
                claims_text += claim_value['value']

                qualifiers = claim_value.get('qualifiers', [])
                if qualifiers:
                    claims_text += qualifiers_to_text(qualifiers)

            properties_text += f"\n- {property_label}: {claims_text}."
        else:
            properties_text += f"\n- есть {property_label}."

    return properties_text
