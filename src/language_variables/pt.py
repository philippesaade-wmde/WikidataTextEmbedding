""" Português """

language = 'pt'

novalue = 'sem valor'

time_variables = {
    'months': ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'],
    'century': 'º século',
    'millennium': 'º milênio',
    'decade': ' década',
    'AD': 'd.C.',
    'BC': 'a.C.',
    'years': 'anos',
    'ten thousand years': 'dez mil anos',
    'hundred thousand years': 'cem mil anos',
    'million years': 'milhão de anos',
    'tens of millions of years': 'dezenas de milhões de anos',
    'hundred million years': 'cento de milhões de anos',
    'billion years': 'bilhão de anos',
}

def merge_entity_text(label, description, aliases, properties):
    """
    Combina os atributos de uma entidade (label, description, aliases, instanceof, properties)
    em uma única string de texto em português.

    Parâmetros:
    - label: string com o rótulo da entidade.
    - description: string com a descrição da entidade.
    - aliases: lista de aliases da entidade.
    - instanceof: lista de tipos (instâncias) da entidade.
    - properties: lista de propriedades da entidade.

    Retorna:
    - Uma string que representa a entidade com sua descrição, aliases e propriedades.
      Se não houver propriedades, o texto termina com ponto final.
    """
    texto = label

    if len(description) > 0:
        texto += f", {description}"

    if len(aliases) > 0:
        texto += f", também conhecido como {', '.join(aliases)}"

    if len(properties) > 0:
        propriedades_texto = properties_to_text(properties)
        texto = f"{texto}. Atributos incluem: {propriedades_texto}"
    else:
        texto = f"{texto}."

    return texto

def qualifiers_to_text(qualifiers):
    """
    Converte uma lista de qualificadores em texto legível.

    Parâmetros:
    - qualifiers: lista de dicionários com qualificadores (property_label e values).

    Retorna:
    - String representando os qualificadores.
    """
    texto = ""
    for qualificador in qualifiers:
        property_label = qualificador['property_label']
        qualifier_values = qualificador['values']
        if qualifier_values:
            if texto:
                texto += " "
            texto += f"({property_label}: {', '.join(qualifier_values)})"
        else:
            if len(text) > 0:
                text += f" "
            texto += f"(possui {property_label})"

    return f" {texto}" if texto else ""

def properties_to_text(properties):
    """
    Converte uma lista de propriedades (claims) em texto legível.

    Parâmetros:
    - properties: lista de dicionários contendo propriedades e seus valores.

    Retorna:
    - String com as propriedades e seus valores.
    """
    texto_propriedades = ""
    for claim in properties:
        property_label = claim['property_label']
        claim_values = claim['values']

        if claim_values:
            valores_txt = ""
            for valor in claim_values:
                if valores_txt:
                    valores_txt += ", "

                valores_txt += valor['value']
                qualificadores = valor.get('qualifiers', [])
                if qualificadores:
                    valores_txt += qualifiers_to_text(qualificadores)

            texto_propriedades += f"\n- {property_label}: {valores_txt}."
        else:
            texto_propriedades += f"\n- possui {property_label}."

    return texto_propriedades
