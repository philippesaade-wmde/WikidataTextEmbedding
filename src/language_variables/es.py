""" Español """

language = 'es'

novalue = 'sin valor'

time_variables = {
    'months': ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'],
    'century': 'siglo',
    'millennium': 'milenio',
    'decade': 'años',
    'AD': 'd.C.',
    'BC': 'a.C.',
    'years': 'años',
    'ten thousand years': 'diez mil años',
    'hundred thousand years': 'cien mil años',
    'million years': 'millón de años',
    'tens of millions of years': 'decenas de millones de años',
    'hundred million years': 'cien millones de años',
    'billion years': 'mil millones de años',
}

def merge_entity_text(label, description, aliases, properties):
    """
    Combina los atributos de la entidad (label, description, aliases, instanceof, properties) en un único texto legible.

    Parámetros:
    - label: Cadena con la etiqueta de la entidad.
    - description: Cadena con la descripción de la entidad.
    - aliases: Diccionario de alias.
    - instanceof: Lista de instancias a las que pertenece la entidad.
    - properties: Diccionario de propiedades.

    Devuelve:
    - Cadena con la representación de la entidad, su descripción, alias y declaraciones.
      Si no hay declaraciones, la descripción termina con punto.
    """
    text = label

    if len(description) > 0:
        text += f", {description}"

    if len(aliases) > 0:
        text += f", también conocido como {', '.join(aliases)}"

    if len(properties) > 0:
        properties_text = properties_to_text(properties)
        text = f"{text}. Sus atributos incluyen: {properties_text}"
    else:
        text = f"{text}."

    return text

def qualifiers_to_text(qualifiers):
    """
    Convierte una lista de calificadores en texto legible.
    Los calificadores añaden información adicional a una declaración.

    Parámetros:
    - qualifiers: Diccionario con los calificadores. Las claves son los IDs de propiedad y los valores son listas con sus valores.

    Devuelve:
    - Cadena representando los calificadores.
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
            if len(text) > 0:
                text += f" "
            text += f"(tiene {property_label})"

    return f" {text}" if len(text) > 0 else ""

def properties_to_text(properties):
    """
    Convierte una lista de propiedades (declaraciones) en texto legible.

    Parámetros:
    - properties: Diccionario de declaraciones. Las claves son IDs de propiedad.

    Devuelve:
    - Cadena con las propiedades y sus valores.
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
                if qualifiers:
                    claims_text += qualifiers_to_text(qualifiers)

            properties_text += f'\n- {property_label}: {claims_text}.'
        else:
            properties_text += f'\n- tiene {property_label}.'

    return properties_text
