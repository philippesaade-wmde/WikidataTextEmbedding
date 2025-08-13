""" Deutsch """

language = 'de'

novalue = 'kein Wert'

time_variables = {
    'months': ['Jan', 'Feb', 'Mär', 'Apr', 'Mai', 'Jun', 'Jul', 'Aug', 'Sep', 'Okt', 'Nov', 'Dez'],
    'century': '. Jahrhundert',
    'millennium': '. Jahrtausend',
    'decade': 'er Jahre',
    'AD': 'n. Chr.',
    'BC': 'v. Chr.',
    'years': 'Jahre',
    'ten thousand years': 'Zehntausend Jahre',
    'hundred thousand years': 'Hunderttausend Jahre',
    'million years': 'Millionen Jahre',
    'tens of millions of years': 'Zehn Millionen Jahre',
    'hundred million years': 'Hundert Millionen Jahre',
    'billion years': 'Milliarden Jahre'
}

def merge_entity_text(label, description, aliases, properties):
    """
    Kombiniert die Entitätsattribute (Label, Beschreibung, Aliase und Eigenschaften) zu einem einzigen Textstring.

    Parameter:
    - label: Ein String, der das Label der Entität darstellt.
    - description: Ein String, der die Beschreibung der Entität darstellt.
    - aliases: Ein Dictionary der Aliase.
    - instanceof: Eine Liste von Klassen, zu denen die Entität gehört.
    - properties: Ein Dictionary der Eigenschaften.

    Rückgabe:
    - Ein String, der die Entität, ihre Beschreibung, das Label, die Aliase und ihre Ansprüche darstellt. Falls keine Ansprüche vorhanden sind, endet die Beschreibung mit einem Punkt.
    """
    text = label

    if len(description) > 0:
        text += f", {description}"

    if len(aliases) > 0:
        text += f", auch bekannt als {', '.join(aliases)}"

    if len(properties) > 0:
        properties_text = properties_to_text(properties)
        text = f"{text}. Attribute umfassen: {properties_text}"
    else:
        text = f"{text}."

    return text

def qualifiers_to_text(qualifiers):
    """
    Konvertiert eine Liste von Qualifikatoren in einen lesbaren Textstring.
    Qualifikatoren bieten zusätzliche Informationen zu einem Anspruch.

    Parameter:
    - qualifiers: Ein Dictionary von Qualifikatoren mit Eigenschafts-IDs als Schlüsseln und deren Werten als Listen.

    Rückgabe:
    - Ein String, der die Qualifikatoren darstellt.
    """
    text = ""
    for claim in qualifiers:
        property_label = claim['property_label']
        qualifier_values = claim['values']
        if (qualifier_values is not None) and len(qualifier_values) > 0:
            if len(text) > 0:
                text += f" "

            text += f"({property_label}: {', '.join(qualifier_values)})"

        else:
            if len(text) > 0:
                text += f" "

            text += f"(hat {property_label})"

    if len(text) > 0:
        return f" {text}"
    return ""

def properties_to_text(properties):
    """
    Konvertiert eine Liste von Eigenschaften (Ansprüchen) in einen lesbaren Textstring.

    Parameter:
    - properties: Ein Dictionary von Eigenschaften (Ansprüchen) mit Eigenschafts-IDs als Schlüsseln.

    Rückgabe:
    - Ein String, der die Eigenschaften und ihre Werte darstellt.
    """
    properties_text = ""
    for claim in properties:
        property_label = claim['property_label']
        claim_values = claim['values']
        if (claim_values is not None) and (len(claim_values) > 0):

            claims_text = ""
            for claim_value in claim_values:
                if len(claims_text) > 0:
                    claims_text += f", "

                claims_text += claim_value['value']

                qualifiers = claim_value.get('qualifiers', [])
                if len(qualifiers) > 0:
                    claims_text += qualifiers_to_text(qualifiers)

            properties_text += f'\n- {property_label}: {claims_text}.'

        else:
            properties_text += f'\n- hat {property_label}.'

    return properties_text