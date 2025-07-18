""" Italiano """

language = 'it'

novalue = 'nessun valore'

time_variables = {
    'months': ['Gen', 'Feb', 'Mar', 'Apr', 'Mag', 'Giu', 'Lug', 'Ago', 'Set', 'Ott', 'Nov', 'Dic'],
    'century': '° secolo',
    'millennium': '° millennio',
    'decade': ' anni',
    'AD': 'd.C.',
    'BC': 'a.C.',
    'years': 'anni',
    'ten thousand years': 'diecimila anni',
    'hundred thousand years': 'centomila anni',
    'million years': 'milioni di anni',
    'tens of millions of years': 'decine di milioni di anni',
    'hundred million years': 'cento milioni di anni',
    'billion years': 'miliardi di anni',
}

def merge_entity_text(label, description, aliases, instanceof, properties):
    """
    Combina le caratteristiche di un'entità (etichetta, descrizione, alias e proprietà) in una stringa.

    Parametri:
    - label: stringa con l'etichetta dell'entità.
    - description: stringa con la descrizione dell'entità.
    - aliases: lista di alias.
    - instanceof: lista di classi di cui l'entità è un'istanza.
    - properties: lista di proprietà.

    Ritorna:
    - Stringa che rappresenta l'entità, la sua descrizione, i suoi alias e le sue proprietà. Se non ci sono proprietà, la descrizione termina con un punto.
    """
    text = label

    if len(instanceof) > 0:
        text += f" ({', '.join(instanceof)})"

    if len(description) > 0:
        text += f", {description}"

    if len(aliases) > 0:
        text += f", noto anche come {', '.join(aliases)}"

    if len(properties) > 0:
        properties_text = properties_to_text(properties)
        text = f"{text}. Gli attributi comprendono: {properties_text}"
    else:
        text = f"{text}."

    return text


def qualifiers_to_text(qualifiers):
    """
    Converte una lista di qualificatori in una stringa leggibile.
    I qualificatori forniscono informazioni aggiuntive su un'affermazione.

    Parametri:
    - qualifiers: lista di qualificatori con etichetta e valori.

    Ritorna:
    - Stringa che rappresenta i qualificatori.
    """
    text = ""
    for claim in qualifiers:
        property_label = claim['property_label']
        qualifier_values = claim['values']
        if qualifier_values and len(qualifier_values) > 0:
            if len(text) > 0:
                text += " "

            text += f"({property_label}: {', '.join(qualifier_values)})"

        else:
            text += f"(ha {property_label})"

    if len(text) > 0:
        return f" {text}"
    return ""


def properties_to_text(properties):
    """
    Converte una lista di proprietà in una stringa leggibile.

    Parametri:
    - properties: lista di proprietà dell'entità con etichetta e valori.

    Ritorna:
    - Stringa che rappresenta le proprietà e i loro valori.
    """
    properties_text = ""
    for claim in properties:
        property_label = claim['property_label']
        claim_values = claim['values']
        if claim_values and len(claim_values) > 0:

            claims_text = ""
            for claim_value in claim_values:
                if len(claims_text) > 0:
                    claims_text += ", "

                claims_text += claim_value['value']

                qualifiers = claim_value.get('qualifiers', [])
                if qualifiers and len(qualifiers) > 0:
                    claims_text += qualifiers_to_text(qualifiers)

            properties_text += f"\n- {property_label}: {claims_text}."

        else:
            properties_text += f"\n- ha {property_label}."

    return properties_text
