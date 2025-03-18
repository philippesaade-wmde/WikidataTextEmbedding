import requests
import time
import json
import re
import importlib

from datetime import date, datetime
from src.wikidataItemDB import WikidataItem

class WikidataTextifier:
    """_summary_
    """
    def __init__(self, language='en', langvar_filename=None):
        """
        Initializes the WikidataTextifier with the specified language.
        Expected use cases: Hugging Face parquet or sqlite database.

        Parameters:
        - language (str): The language code used by the textifier
            Default is "en".
        """

        self.language = language
        langvar_filename = (
            langvar_filename if langvar_filename is not None else language
        )
        try:
            # Importing custom functions and variables
            # from a formating python script in the language_variables folder.
            self.langvar = importlib.import_module(
                f"src.language_variables.{langvar_filename}"
            )
        except Exception as e:
            raise ValueError(f"Language file for '{language}' not found.")

    def get_label(self, id, labels=None):
        """Retrieves the label for a Wikidata entity in a specified language.

        Args:
            id (str): QID or PID from the ID column in the Wikidata db or JSON.
            labels (dict, optional): Wikidata labels in all available languages, else None. Defaults to None.

        Returns:
            str: Wikidata label from specified language or mul[tilingual].
        """
        if (labels is None) or (len(labels) == 0):
            # If the labels are not provided, fetch them from the Wikidata SQLDB
            # TODO: Fetch from the Wikidata API if not found in the SQLDB
            labels = WikidataItem.get_labels(id)

        if isinstance(labels, str):
            # If the labels are a string, return them as is
            return labels

        # Take the label from the language, if missing take it 
        # from the multiligual class

        label = labels.get(self.language)
        if label is None:
            label = labels.get('mul')

        if isinstance(label, dict):
            label = label.get('value')

        return label

    def get_description(self, id, descriptions=None):
        """Retrieves the description for a Wikidata entity 
            in the specified language.

        Args:
            id (str): QID or PID from the ID column in the Wikidata db or JSON.
            descriptions (dict, optional): Wikidata descriptions in all available languages, else None. Defaults to None.

        Returns:
            str: Wikidata description from specified language or mul[tilingual].
        """
        if (descriptions is None) or (len(descriptions) == 0):
            # If the descriptions are not provided, fetch them from the Wikidata SQLDB
            # TODO: Fetch from the Wikidata API if not found in the SQLDB
            descriptions = WikidataItem.get_descriptions(id)

        if isinstance(descriptions, str):
            return descriptions


        # Take the description from the language,
        # if missing take it from the multiligual class
        description = descriptions.get(self.language)
        if description is None:
            description = descriptions.get('mul')

        if isinstance(description, dict):
            description = description.get('value')

        return description


    def get_aliases(self, aliases):
        """Retrieves the aliases for a Wikidata entity in the specified language.

        Args:
            aliases (dict, optional): Wikidata aliases in all available languages, else None. Defaults to None.

        Returns:
            list: Wikidata aliases from specified language and mul[tilingual].
        """
        if (type(aliases) is list):
            return aliases

        if aliases is None:
            return []

        # Combine the aliases from the specified language and the
        # multilingual class. Use set format to avoid duplicates.
        aliases = set()
        if self.language in aliases:
            aliases.update([x['value'] for x in aliases[self.language]])

        if 'mul' in aliases:
            aliases.update([x['value'] for x in aliases['mul']])

        return list(aliases)

    def entity_to_text(self, entity, properties=None):
        """Converts a Wikidata entity into a human-readable text string.

        Args:
            entity (obj):  A Wikidata entity object containing
                entity data (label, description, claims, etc.)
            properties (dict or None, optional): A dictionary of
                properties (claims). If None, the properties will be derived
                from entity.claims. Defaults to None.

        Returns:
            (str): A human-readable representation of the entity, its description, aliases, and claims.
        """
        if properties is None:
            # If properties are not provided, fetch them from the entity
            properties = self.properties_to_dict(entity.claims)

        # Get the label, description, and aliases for the entity
        label = self.get_label(entity.id, labels=entity.label)

        description = self.get_description(
            entity.id,
            descriptions=entity.description
        )
        if (description is None) or (len(description) == 0):
            # If the description is missing, try to get it
            # from the `instance_of` property
            instanceof = self.get_label('P31')
            description = properties.get(instanceof, '')

        aliases = self.get_aliases(entity.aliases)

        # Merge the label, description, aliases, and properties into a single
        # text string as the Data Model per language through langvar descriptors
        return self.langvar.merge_entity_text(
            label,
            description,
            aliases,
            properties
        )

    def properties_to_dict(self, properties):
        """
        Converts a dictionary of properties (claims) into a dict suitable for text generation.

        Parameters:
        - properties (dict): A dictionary of claims keyed by property IDs.
            Each value is a list of claim statements for that property.

        Returns:
        - dict: A dictionary mapping property labels to a list of
            their parsed values (and qualifiers).
        """
        properties_dict = {}
        for pid, claim in properties.items():
            p_data = []
            rank_preferred_found = False

            for c in claim:
                try:
                    value = self.mainsnak_to_value(c.get('mainsnak', c))
                    qualifiers = self.qualifiers_to_dict(
                        c.get('qualifiers', {})
                    )
                    rank = c.get('rank', 'normal').lower()

                    if value is None:
                        p_data = None
                        break

                    elif len(value) > 0:
                        # If a preferred rank exists, include values that are
                        # only preferred. Else include only values that are
                        # ranked normal (values with a depricated rank are
                        # never included)
                        if ((not rank_preferred_found) and (rank == 'normal')) or (rank == 'preferred'):
                            if (not rank_preferred_found) and (rank == 'preferred'):
                                rank_preferred_found = True
                                p_data = []

                            p_data.append({'value': value, 'qualifiers': qualifiers})
                except Exception as e:
                    print(c)
                    raise e

            label = self.get_label(pid, claim[0].get('mainsnak', {}).get('property-labels'))
            if label:
                properties_dict[label] = p_data

        return properties_dict

    def qualifiers_to_dict(self, qualifiers):
        """
        Converts qualifiers into a dictionary suitable for text generation.

        Parameters:
        - qualifiers (dict): A dictionary of qualifiers keyed by property IDs.
                             Each value is a list of qualifier statements.

        Returns:
        - dict: A dictionary mapping property labels to a list of their parsed values.
        """
        qualifier_dict = {}
        for pid, qualifier in qualifiers.items():
            q_data = []

            for q in qualifier:
                value = self.mainsnak_to_value(q)
                if value is None:
                    q_data = None
                    break
                elif len(value) > 0:
                    q_data.append(value)

            label = self.get_label(pid, qualifier[0].get('property-labels'))
            if label:
                qualifier_dict[label] = q_data

        return qualifier_dict

    def mainsnak_to_value(self, mainsnak):
        """
        Converts a Wikidata 'mainsnak' object into a human-readable value string. This method interprets various datatypes (e.g., wikibase-item, string, time, quantity) and returns a formatted text representation.

        Parameters:
        - mainsnak (dict): A snak object containing the value and datatype information.

        Returns:
        - str or None: A string representation of the value. If the returned string is empty, the value is discarded from the text, and If None i retured, then the whole property is discarded.
        """
        # Extract the datavalue
        snaktype = mainsnak.get('snaktype', 'value')
        datavalue = mainsnak.get('datavalue')
        if (datavalue is not None) and (type(datavalue) is not str):
            datavalue = datavalue.get('value', datavalue)

        # Consider missing values
        if (snaktype != 'value') or (datavalue is None):
            return self.langvar.novalue

        # If the values is based on a language, only consider the language that matched the text representation language.
        elif (type(datavalue) is dict) and ('language' in datavalue) and (datavalue['language'] != self.language):
            return ''

        elif (mainsnak.get('datatype', '') == 'wikibase-item') or (mainsnak.get('datatype', '') == 'wikibase-property'):
            if type(datavalue) is str:
                return self.get_label(datavalue)

            entity_id = datavalue['id']
            label = self.get_label(entity_id, datavalue.get('labels'))
            return label

        elif mainsnak.get('datatype', '') == 'monolingualtext':
            return datavalue.get('text', datavalue)

        elif mainsnak.get('datatype', '') == 'string':
            return datavalue

        elif mainsnak.get('datatype', '') == 'time':
            try:
                return self.time_to_text(datavalue)
            except Exception as e:
                print("Error in time formating:", e)
                return datavalue["time"]

        elif mainsnak.get('datatype', '') == 'quantity':
            try:
                return self.quantity_to_text(datavalue)
            except Exception as e:
                print(e)
                return datavalue['amount']

        elif mainsnak.get('datatype', '') == 'external-id':
            return None

        else:
            return ''

    def quantity_to_text(self, quantity_data):
        """
        Converts Wikidata quantity data into a human-readable string.

        Parameters:
        - quantity_data (dict): A dictionary with 'amount' and optionally 'unit' (often a QID).

        Returns:
        - str: A textual representation of the quantity (e.g., "5 kg").
        """
        if quantity_data is None:
            return None

        quantity = quantity_data.get('amount')
        unit = quantity_data.get('unit')

        # 'unit' of '1' means that the value is a count and doesn't require a unit.
        if unit == '1':
            unit = None
        else:
            unit_qid = unit.rsplit('/')[-1]
            unit = self.get_label(unit_qid, quantity_data.get('unit-labels'))

        return quantity + (f" {unit}" if unit else "")

    def time_to_text(self, time_data):
        """
        Converts Wikidata time data into a human-readable string.

        Parameters:
        - time_data (dict): A dictionary containing the time string, precision, and calendar model.

        Returns:
        - str: A textual representation of the time with appropriate granularity.
        """
        if time_data is None:
            return None

        time_value = time_data['time']
        precision = time_data['precision']
        calendarmodel = time_data.get('calendarmodel', 'http://www.wikidata.org/entity/Q1985786')

        # Use regex to parse the time string
        pattern = r'([+-])(\d{1,16})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z'
        match = re.match(pattern, time_value)

        if not match:
            raise ValueError("Malformed time string")

        sign, year_str, month_str, day_str, hour_str, minute_str, second_str = match.groups()
        year = int(year_str) * (1 if sign == '+' else -1)

        # Convert Julian to Gregorian if necessary
        if 'Q1985786' in calendarmodel and year > 1 and len(str(abs(year))) <= 4:  # Julian calendar
            try:
                month = 1 if month_str == '00' else int(month_str)
                day = 1 if day_str == '00' else int(day_str)
                julian_date = date(year, month, day)
                gregorian_ordinal = julian_date.toordinal() + (datetime(1582, 10, 15).toordinal() - datetime(1582, 10, 5).toordinal())
                gregorian_date = date.fromordinal(gregorian_ordinal)
                year, month, day = gregorian_date.year, gregorian_date.month, gregorian_date.day
            except ValueError:
                raise ValueError("Invalid date for Julian calendar")
        else:
            month = int(month_str) if month_str != '00' else 1
            day = int(day_str) if day_str != '00' else 1

        month_str = self.langvar.time_variables['months'][month - 1] if month != 0 else ''
        ad = self.langvar.time_variables['AD']
        bc = self.langvar.time_variables['BC']

        if precision == 14:
            return f"{year} {month_str} {day} {hour_str}:{minute_str}:{second_str}"
        elif precision == 13:
            return f"{year} {month_str} {day} {hour_str}:{minute_str}"
        elif precision == 12:
            return f"{year} {month_str} {day} {hour_str}:00"
        elif precision == 11:
            return f"{day} {month_str} {year}"
        elif precision == 10:
            return f"{month_str} {year}"
        elif precision == 9:
            era = '' if year > 0 else f' {bc}'
            return f"{abs(year)}{era}"
        elif precision == 8:
            decade = (year // 10) * 10
            decade_suffix = self.langvar.time_variables['decade']
            era = ad if year > 0 else bc
            return f"{abs(decade)}{decade_suffix} {era}"
        elif precision == 7:
            century = (abs(year) - 1) // 100 + 1
            era = ad if year > 0 else bc
            return f"{century}{self.langvar.time_variables['century']} {era}"
        elif precision == 6:
            millennium = (abs(year) - 1) // 1000 + 1
            era = ad if year > 0 else bc
            return f"{millennium}{self.langvar.time_variables['millennium']} {era}"
        elif precision == 5:
            tens_of_thousands = abs(year) // 10000
            era = ad if year > 0 else bc
            return f"{tens_of_thousands} {self.langvar.time_variables['ten thousand years']} {era}"
        elif precision == 4:
            hundreds_of_thousands = abs(year) // 100000
            era = ad if year > 0 else bc
            return f"{hundreds_of_thousands} {self.langvar.time_variables['hundred thousand years']} {era}"
        elif precision == 3:
            millions = abs(year) // 1000000
            era = ad if year > 0 else bc
            return f"{millions} {self.langvar.time_variables['million years']} {era}"
        elif precision == 2:
            tens_of_millions = abs(year) // 10000000
            era = ad if year > 0 else bc
            return f"{tens_of_millions} {self.langvar.time_variables['tens of millions of years']} {era}"
        elif precision == 1:
            hundreds_of_millions = abs(year) // 100000000
            era = ad if year > 0 else bc
            return f"{hundreds_of_millions} {self.langvar.time_variables['hundred million years']} {era}"
        elif precision == 0:
            billions = abs(year) // 1000000000
            era = ad if year > 0 else bc
            return f"{billions} {self.langvar.time_variables['billion years']} {era}"
        else:
            raise ValueError(f"Unknown precision value {precision}")

    def data_to_text(self, data, datatype):
        """
        Converts specific Wikidata data (time or quantity) into a string using the Wikidata API. Ideally, this function should replace "time_to_text" and "quantity_to_text", however it's too slow.

        Parameters:
        - data (dict): The dictionary structure of the datavalue (time or quantity).
        - datatype (str): The datatype (usually 'time' or 'quantity').

        Returns:
        - str: The formatted value (as returned by the Wikidata API).
        """
        while True:
            try:
                data = {
                    'action': 'wbformatvalue',
                    'format': 'json',
                    'datavalue': json.dumps(data),
                    'datatype': datatype,
                    'uselang': self.langvar.language,
                    'formatversion': 2
                }
                r = requests.get('https://www.wikidata.org/w/api.php', params=data)
                return r.json()['result']
            except Exception as e:
                print(e)
                while True:
                    try:
                        response = requests.get("https://www.google.com", timeout=5)
                        if response.status_code == 200:
                            break
                    except Exception as e:
                        print("Waiting for internet connection...")
                        time.sleep(5)

    def chunk_text(self, entity, tokenizer, max_length=500):
        """
        Splits a text representation of an entity into smaller chunks so that each chunk fits within the token limit of a given tokenizer.

        Parameters:
        - entity: The entity to be textified and chunked.
        - tokenizer: A tokenizer (e.g. from Hugging Face) used to count tokens.
        - max_length (int): The maximum number of tokens allowed per chunk (default is 500).

        Returns:
        - list[str]: A list of text chunks, each within the token limit.
        """
        entity_text = self.entity_to_text(entity)
        max_length = max_length

        # If the full text does not exceed the maximum tokens then we only return 1 chunk.
        tokens = tokenizer(entity_text, add_special_tokens=False, return_offsets_mapping=True)
        if len(tokens['input_ids']) < max_length:
            return [entity_text]

        # If the label and description already exceed the maximum tokens then we will truncate it and will not include chunks that include claims.
        entity_description= self.entity_to_text(entity, properties={})
        tokens = tokenizer(entity_description, add_special_tokens=False, return_offsets_mapping=True)
        token_ids, offsets = tokens['input_ids'], tokens['offset_mapping']
        if len(token_ids) >= max_length:
            start, end = offsets[0][0], offsets[max_length - 1][1]
            return [entity_text[start:end]]  # Return the truncated portion of the original text

        # Create the chunks assuming the description/label text is smaller than the maximum tokens.
        properties = self.properties_to_dict(entity.claims)
        chunks = []
        chunk_claims = {}
        for claim, value in properties.items():
            current_chunk_claims = {**chunk_claims, claim: value}
            entity_text = self.entity_to_text(entity, current_chunk_claims)
            tokens = tokenizer(entity_text, add_special_tokens=False, return_offsets_mapping=True)

            # Check when including the current claim if we exceed the maximum tokens.
            if len(tokens['input_ids']) >= max_length:
                start, end = tokens['offset_mapping'][0][0], tokens['offset_mapping'][max_length - 1][1]
                chunks.append(entity_text[start:end])

                # If we do exceed it but there's no claim previously added to the chunks, then it means the current claim alone exceeds the maximum tokens, and we already included it in a trimmed chunk alone.
                if len(chunk_claims) == 0:
                    chunk_claims = {}

                # Include the claim in a new chunk so where it's information doesn't get trimmed.
                else:
                    chunk_claims = {claim: value}
            else:
                chunk_claims = current_chunk_claims

        # Add the final chunk if any claims remain
        if len(chunk_claims) > 0:
            entity_text = self.entity_to_text(entity, chunk_claims)
            tokens = tokenizer(entity_text, add_special_tokens=False, return_offsets_mapping=True)

            if len(tokens['input_ids']) >= max_length:
                start, end = tokens['offset_mapping'][0][0], tokens['offset_mapping'][max_length - 1][1]
            else:
                start, end = tokens['offset_mapping'][0][0], tokens['offset_mapping'][-1][1]
            chunks.append(entity_text[start:end])

        return chunks