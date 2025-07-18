""" English """

language = 'en'

novalue = 'no value'

time_variables = {
    'months': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
    'century': 'th century',
    'millennium': 'th millennium',
    'decade': 's',
    'AD': 'AD',
    'BC': 'BC',
    'years': 'years',
    'ten thousand years': 'ten thousand years',
    'hundred thousand years': 'hundred thousand years',
    'million years': 'million years',
    'tens of millions of years': 'tens of millions of years',
    'hundred million years': 'hundred million years',
    'billion years': 'billion years',
}

def merge_entity_text(label, description, aliases, instanceof, properties):
    """
    Combines the entity attributes (label, description, aliases, and properties) into a single text string.

    Parameters:
    - label: A string representing the entity's label.
    - description: A string representing the entity's description.
    - aliases: A dictionary of aliases.
    - instanceof: A list of instance of values.
    - properties: A dictionary of properties.

    Returns:
    - A string representation of the entity, its description, label, aliases, and its claims. If there are no claims, the description ends with a period.
    """
    text = label

    if len(instanceof) > 0:
        text += f" ({', '.join(instanceof)})"

    if len(description) > 0:
        text += f", {description}"

    if len(aliases) > 0:
        text += f", also known as {', '.join(aliases)}"

    if len(properties) > 0:
        properties_text = properties_to_text(properties)
        text = f"{text}. Attributes include: {properties_text}"
    else:
        text = f"{text}."

    return text

def qualifiers_to_text(qualifiers):
    """
    Converts a list of qualifiers to a readable text string.
    Qualifiers provide additional information about a claim.

    Parameters:
    - qualifiers: A dictionary of qualifiers with property IDs as keys and their values as lists.

    Returns:
    - A string representation of the qualifiers.
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
            text += f"(has {property_label})"

    if len(text) > 0:
        return f" {text}"
    return ""

def properties_to_text(properties, include_subject=False):
    """
    Converts a list of properties (claims) to a readable text string.

    Parameters:
    - properties: A dictionary of properties (claims) with property IDs as keys.

    Returns:
    - A string representation of the properties and their values.
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

            if include_subject:
                subject_label = claim['subject_label']
                properties_text += f'\n- {subject_label}: {property_label}: {claims_text}.'
            else:
                properties_text += f'\n- {property_label}: {claims_text}.'

        else:
            if include_subject:
                subject_label = claim['subject_label']
                properties_text += f'\n- {subject_label}: has {property_label}.'
            else:
                properties_text += f'\n- has {property_label}.'

    return properties_text

def merge_property_text(label, description, aliases, examples):
    """
    Combines the property attributes (label, description, aliases, and examples) into a single text string.

    Parameters:
    - label: A string representing the entity's label.
    - description: A string representing the entity's description.
    - aliases: A dictionary of aliases.
    - examples: A dictionary of claims that include examples of the usage of the property.

    Returns:
    - A string representation of the entity, its description, label, aliases, and examples.
    """

    # Same as merge_entity_text but without instanceof or properties
    text = merge_entity_text(label, description, aliases, [], [])

    if len(examples) > 0:
        examples_text = properties_to_text(examples, include_subject=True)
        text = f"{text} Examples: {examples_text}"

    return text