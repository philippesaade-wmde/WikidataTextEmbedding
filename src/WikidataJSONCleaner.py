"""Clean Wikidata JSON entities for downstream text and dataset publishing."""

import re


class WikidataJSONCleaner:
    """Clean and normalize nested Wikidata entity structures."""

    @staticmethod
    def clean_label_description(data):
        """Return language-to-value mappings for labels or descriptions."""
        clean_data = {}
        for lang, label in data.items():
            clean_data[lang] = label["value"]
        return clean_data

    @staticmethod
    def clean_aliases(data):
        """Return language-to-alias-list mappings."""
        clean_data = {}
        for lang, aliases in data.items():
            clean_data[lang] = [a["value"] for a in aliases]
        return clean_data

    @staticmethod
    def is_in_wikipedia(entity):
        """Return whether an entity has any Wikipedia sitelink."""
        if "sitelinks" in entity:
            for s in entity["sitelinks"]:
                if s.endswith("wiki"):
                    return True
        return False

    @staticmethod
    def _remove_keys(data, keys_to_remove=None):
        """Recursively remove specific keys from a nested data structure."""
        if keys_to_remove is None:
            keys_to_remove = ("hash", "property", "numeric-id", "qualifiers-order")
        if isinstance(data, dict):
            data = {
                key: WikidataJSONCleaner._remove_keys(value, keys_to_remove)
                for key, value in data.items()
                if key not in keys_to_remove
            }
        elif isinstance(data, list):
            data = [
                WikidataJSONCleaner._remove_keys(item, keys_to_remove) for item in data
            ]
        return data

    @staticmethod
    def _clean_datavalue(data):
        """Remove wrapper structures unless they look like Wikidata IDs."""
        if isinstance(data, dict):
            # If there's only one key and it's not a property or QID, recurse into it.
            if len(data) == 1 and not re.match(r"^[PQ]\d+$", key := next(iter(data))):
                data = WikidataJSONCleaner._clean_datavalue(data[key])
            else:
                data = {
                    key: WikidataJSONCleaner._clean_datavalue(value)
                    for key, value in data.items()
                }
        elif isinstance(data, list):
            data = [WikidataJSONCleaner._clean_datavalue(item) for item in data]
        return data

    @staticmethod
    def _gather_labels_ids(data):
        """Find relevant Wikidata IDs in nested claims data."""
        ids = set()

        if isinstance(data, dict):
            if "property" in data:
                ids.add(data["property"])

            if "unit" in data and data["unit"] != "1":
                unit_id = data["unit"].split("/")[-1]
                ids.add(unit_id)

            if (
                "datatype" in data
                and "datavalue" in data
                and data["datatype"] in ("wikibase-item", "wikibase-property")
            ):
                print(data["datavalue"])
                ids.add(data["datavalue"])

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
        """Insert resolved labels for property, unit, and datavalue IDs."""
        if isinstance(data, dict):
            if "property" in data:
                labels = get_labels_func(data["property"])

                data = {**data, "property-labels": labels}

            if ("unit" in data) and (data["unit"] != "1"):
                id = data["unit"].split("/")[-1]
                labels = get_labels_func(id)

                data = {**data, "unit-labels": labels}

            if (
                ("datatype" in data)
                and ("datavalue" in data)
                and (data["datatype"] in ["wikibase-item", "wikibase-property"])
            ):
                labels = get_labels_func(data["datavalue"])

                data["datavalue"] = {"id": data["datavalue"], "labels": labels}

            data = {
                key: WikidataJSONCleaner._add_labels_to_claims(value, get_labels_func)
                for key, value in data.items()
            }

        elif isinstance(data, list):
            data = [
                WikidataJSONCleaner._add_labels_to_claims(item, get_labels_func)
                for item in data
            ]

        return data

    @staticmethod
    def clean_entity(entity, get_labels_func):
        """Return a compact entity dictionary with labels added to claims."""
        clean_claims = WikidataJSONCleaner._remove_keys(
            entity.get("claims", {}),
            [
                "hash",
                "snaktype",
                "type",
                "entity-type",
                "numeric-id",
                "qualifiers-order",
                "snaks-order",
            ],
        )
        clean_claims = WikidataJSONCleaner._clean_datavalue(clean_claims)
        clean_claims = WikidataJSONCleaner._remove_keys(clean_claims, ["id"])
        clean_claims = WikidataJSONCleaner._add_labels_to_claims(
            clean_claims,
            get_labels_func,
        )

        sitelinks = WikidataJSONCleaner._remove_keys(
            entity.get("sitelinks", {}), ["badges"]
        )

        return {
            "id": entity.get("id", ""),
            "labels": WikidataJSONCleaner.clean_label_description(
                entity.get("labels", {})
            ),
            "descriptions": WikidataJSONCleaner.clean_label_description(
                entity.get("descriptions", {})
            ),
            "aliases": WikidataJSONCleaner.clean_aliases(entity.get("aliases", {})),
            "sitelinks": sitelinks,
            "claims": clean_claims,
        }
