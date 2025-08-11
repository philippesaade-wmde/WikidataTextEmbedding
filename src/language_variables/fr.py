""" Français """

language = 'fr'

novalue = 'pas de valeur'

time_variables = {
    'months': ['Janv', 'Févr', 'Mars', 'Avr', 'Mai', 'Juin', 'Juil', 'Août', 'Sept', 'Oct', 'Nov', 'Déc'],
    'century': 'e siècle',
    'millennium': 'e millénaire',
    'decade': 'années',
    'AD': 'apr. J.-C.',
    'BC': 'av. J.-C.',
    'years': 'ans',
    'ten thousand years': 'dix mille ans',
    'hundred thousand years': 'cent mille ans',
    'million years': 'un million d\'années',
    'tens of millions of years': 'dizaines de millions d\'années',
    'hundred million years': 'cent millions d\'années',
    'billion years': 'un milliard d\'années',
}

def merge_entity_text(label, description, aliases, properties):
    """
    Combine les attributs d’une entité (label, description, alias et propriétés)
    en une seule chaîne de texte lisible.

    Paramètres :
    - label : chaîne représentant le label de l’entité.
    - description : chaîne représentant la description.
    - aliases : liste ou dictionnaire des alias.
    - instanceof : liste des instances auxquelles l’entité appartient.
    - properties : dictionnaire des propriétés.

    Retour :
    - Chaîne décrivant l’entité, sa description, ses alias et ses revendications.
    """
    text = label

    if len(description) > 0:
        text += f", {description}"

    if len(aliases) > 0:
        text += f", également connu sous le nom de {', '.join(aliases)}"

    if len(properties) > 0:
        properties_text = properties_to_text(properties)
        text = f"{text}. Ses attributs incluent : {properties_text}"
    else:
        text = f"{text}."

    return text

def qualifiers_to_text(qualifiers):
    """
    Convertit une liste de qualificatifs en texte lisible.

    Paramètres :
    - qualifiers : liste de dictionnaires, chaque dictionnaire contenant 'property_label' et 'values'.

    Retour :
    - Chaîne représentant les qualificatifs.
    """
    text = ""
    for claim in qualifiers:
        property_label = claim['property_label']
        qualifier_values = claim['values']
        if qualifier_values and len(qualifier_values) > 0:
            if len(text) > 0:
                text += " "
            text += f"({property_label} : {', '.join(qualifier_values)})"
        else:
            text += f"(possède {property_label})"

    return f" {text}" if text else ""

def properties_to_text(properties):
    """
    Transforme une liste de propriétés (revendications) en texte lisible.

    Paramètres :
    - properties : liste ou dictionnaire de propriétés contenant 'property_label' et 'values'.

    Retour :
    - Chaîne représentant les propriétés et leurs valeurs.
    """
    properties_text = ""
    for claim in properties:
        property_label = claim['property_label']
        claim_values = claim['values']
        if claim_values and len(claim_values) > 0:
            claims_text = ""
            for claim_value in claim_values:
                if claims_text:
                    claims_text += ", "
                claims_text += claim_value['value']
                qualifiers = claim_value.get('qualifiers', [])
                if qualifiers:
                    claims_text += qualifiers_to_text(qualifiers)
            properties_text += f"\n- {property_label} : {claims_text}."
        else:
            properties_text += f"\n- possède {property_label}."
    return properties_text
