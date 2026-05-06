import re

class WikidataJSONCleaner:
    """ Represents a Wikidata entity's labels in multiple languages."""

    @staticmethod
    def clean_label_description(data):
        clean_data = {}
        for lang, label in data.items():
            clean_data[lang] = label['value']
        return clean_data

    @staticmethod
    def clean_aliases(data):
        clean_data = {}
        for lang, aliases in data.items():
            clean_data[lang] = [a['value'] for a in aliases]
        return clean_data

    @staticmethod
    def is_in_wikipedia(entity):
        """
        Check if a Wikidata entity has a corresponding Wikipedia entry in any language.

        Parameters:
        - entity (dict): A Wikidata entity dictionary.

        Returns:
        - bool: True if the entity has at least one sitelink ending in 'wiki', otherwise False.
        """
        if ('sitelinks' in entity):
            for s in entity['sitelinks']:
                if s.endswith('wiki'):
                    return True
        return False

    @staticmethod
    def _remove_keys(data, keys_to_remove=['hash', 'property', 'numeric-id', 'qualifiers-order']):
        """
        Recursively remove specific keys from a nested data structure.

        Parameters:
        - data (dict or list): The data structure to clean.
        - keys_to_remove (list): Keys to remove. Default includes 'hash', 'property', 'numeric-id', and 'qualifiers-order'.

        Returns:
        - dict or list: The cleaned data structure with specified keys removed.
        """
        if isinstance(data, dict):
            data = {key: WikidataJSONCleaner._remove_keys(value, keys_to_remove) for key, value in data.items() if key not in keys_to_remove}
        elif isinstance(data, list):
            data = [WikidataJSONCleaner._remove_keys(item, keys_to_remove) for item in data]
        return data

    @staticmethod
    def _clean_datavalue(data):
        """
        Remove unnecessary nested structures unless they match a Wikidata entity or property pattern.

        Parameters:
        - data (dict or list): The data structure to clean.

        Returns:
        - dict or list: The cleaned data.
        """
        if isinstance(data, dict):
            # If there's only one key and it's not a property or QID, recurse into it.
            if (len(data.keys()) == 1) and not re.match(r"^[PQ]\d+$", list(data.keys())[0]):
                data = WikidataJSONCleaner._clean_datavalue(data[list(data.keys())[0]])
            else:
                data = {key: WikidataJSONCleaner._clean_datavalue(value) for key, value in data.items()}
        elif isinstance(data, list):
            data = [WikidataJSONCleaner._clean_datavalue(item) for item in data]
        return data

    @staticmethod
    def _gather_labels_ids(data):
        """
        Find and return all relevant Wikidata IDs (e.g., property, unit, or datavalue IDs) in the claims.

        Parameters:
        - data (dict or list): The data structure to scan.

        Returns:
        - list[str]: A list of discovered Wikidata IDs.
        """
        ids = set()

        if isinstance(data, dict):
            if 'property' in data:
                ids.add(data['property'])

            if 'unit' in data and data['unit'] != '1':
                unit_id = data['unit'].split('/')[-1]
                ids.add(unit_id)

            if 'datatype' in data \
                and 'datavalue' in data \
                    and data['datatype'] in (
                        'wikibase-item', 'wikibase-property'
                    ):
                print(data['datavalue'])
                ids.add(data['datavalue'])

            for value in data.values():
                sub_ids = WikidataJSONCleaner._gather_labels_ids(value)
                ids.update(sub_ids)

        elif isinstance(data, list):
            for item in data:
                sub_ids = WikidataJSONCleaner._gather_labels_ids(item)
                ids.update(sub_ids)

        return list(ids)

    @staticmethod
    def _add_labels_to_claims(data, get_labels_func):
        """
        For each found ID (property, unit, or datavalue) within the claims,
        insert the corresponding labels from labels_dict or the database.

        Parameters:
        - data (dict or list): The claims data structure.
        - labels_dict (dict): An optional dict of {id: labels} for quick lookup.

        Returns:
        - dict or list: The updated data with added label information.
        """
        if isinstance(data, dict):
            if 'property' in data:
                labels = get_labels_func(data['property'])

                data = {
                    **data,
                    'property-labels': labels
                }

            if ('unit' in data) and (data['unit'] != '1'):
                id = data['unit'].split('/')[-1]
                labels = get_labels_func(id)

                data = {
                    **data,
                    'unit-labels': labels
                }

            if ('datatype' in data) and ('datavalue' in data) and \
                (data['datatype'] in ['wikibase-item', 'wikibase-property']):
                labels = get_labels_func(data['datavalue'])

                data['datavalue'] = {
                    'id': data['datavalue'],
                    'labels': labels
                }

            data = {key: WikidataJSONCleaner._add_labels_to_claims(value, get_labels_func) for key, value in data.items()}

        elif isinstance(data, list):
            data = [WikidataJSONCleaner._add_labels_to_claims(item, get_labels_func) for item in data]

        return data

    @staticmethod
    def clean_entity(entity, get_labels_func):
        """
        Clean a Wikidata entity's data by removing unneeded keys and adding label info to claims.

        Parameters:
        - entity (dict): A Wikidata entity dictionary containing 'claims', 'labels', 'sitelinks', etc.

        Returns:
        - dict: The cleaned entity with label data integrated into its claims.
        """
        clean_claims = WikidataJSONCleaner._remove_keys(entity.get('claims', {}), ['hash', 'snaktype', 'type', 'entity-type', 'numeric-id', 'qualifiers-order', 'snaks-order'])
        clean_claims = WikidataJSONCleaner._clean_datavalue(clean_claims)
        clean_claims = WikidataJSONCleaner._remove_keys(clean_claims, ['id'])
        clean_claims = WikidataJSONCleaner._add_labels_to_claims(
            clean_claims,
            get_labels_func,
        )

        sitelinks = WikidataJSONCleaner._remove_keys(entity.get('sitelinks', {}), ['badges'])

        return {
            'id': entity.get('id', ''),
            'labels': WikidataJSONCleaner.clean_label_description(
                entity.get('labels', {})
            ),
            'descriptions': WikidataJSONCleaner.clean_label_description(
                entity.get('descriptions', {})
            ),
            'aliases': WikidataJSONCleaner.clean_aliases(entity.get('aliases', {})),
            'sitelinks': sitelinks,
            'claims': clean_claims
        }