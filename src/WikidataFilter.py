import requests
from bs4 import BeautifulSoup
import os
import json

class WikidataPropertyFilter:
    """Class to fetch and store the sorted properties."""

    def __init__(self, data_dict="./data/"):
        """Initialize the class with the path to store the sorted properties."""
        self.data_dict = data_dict
        self.sorted_property_url = "https://www.wikidata.org/wiki/MediaWiki:Wikibase-SortedProperties"

        self.property_sort = None
        self.keyword_filters = {
            'Sandbox',
            'entity-schema',
            'geo-shape',
            'musical-notation',
            'wikibase-sense',
            'wikibase-form',
            'wikibase-lexeme',
            'tabular data',
            'external-id',
            'commons',
            'categories',
            'images'
        }
        self._load()

    def get_property_sort(self):
        """Fetch the sorted properties from the Wikidata page and store them in a dictionary."""
        self.property_sort = {}
        pid_count = 0
        hierarchy = {}

        r = requests.get(
            "https://www.wikidata.org/wiki/MediaWiki:Wikibase-SortedProperties",
            headers={"User-Agent": "Wikidata Vector Embedding (embedding@wikimedia.org)"}
        )
        soup = BeautifulSoup(r.text, 'html.parser')
        content = soup.find('div', class_='mw-content-ltr').find('meta')

        # Iterate over all children inside the main content
        for section in content.children:
            if section.name and 'class' in section.attrs and 'mw-heading' in section.attrs['class']:
                # Determine heading level (h2, h3, etc.)
                heading_tag = section.find(['h2', 'h3', 'h4', 'h5', 'h6'])
                if heading_tag:
                    level = int(heading_tag.name[1])  # Extract number from tag (h2 -> 2, h3 -> 3, etc.)
                    title_text = heading_tag.get_text(strip=True)

                    # Update hierarchy
                    hierarchy[level] = title_text

                    # Remove deeper levels when moving up
                    hierarchy = {k: v for k, v in hierarchy.items() if k <= level}

            elif section.name in ['ul', 'ol']:
                # Extract PIDs
                pids = [li.text.split()[0] for li in section.find_all("li")]

                # Construct hierarchical title
                hierarchical_title = " > ".join(hierarchy.values())

                # Store data
                for pid in pids:
                    self.property_sort[pid] = {
                        "hierarchical_title": hierarchical_title,
                        "sort_index": pid_count
                    }
                    pid_count += 1

        # Save to JSON
        self._save()

    def get_sort_index(self, pid):
        """Get the sort index for a given property ID (PID)."""
        if self.property_sort is None:
            self.get_property_sort()

        return self.property_sort.get(pid, {}).get("sort_index", float('inf'))

    def filter(self, pid):
        """Determine if a property should be filtered out based on keywords."""
        if self.property_sort is None:
            self.get_property_sort()

        hierarchical_title = self.property_sort.get(pid, {}).get("hierarchical_title", "").lower()
        return any(keyword in hierarchical_title for keyword in self.keyword_filters)

    def _save(self):
        """Save the sorted properties to a JSON file."""
        if self.property_sort is not None:
            with open(self.data_dict + "property_sort.json", "w") as f:
                json.dump(self.property_sort, f, indent=4)

    def _load(self):
        """Load the sorted properties from a JSON file if it exists."""
        if os.path.exists(self.data_dict + "property_sort.json"):
            with open(self.data_dict + "property_sort.json", "r") as f:
                self.property_sort = json.load(f)

    def sort_and_filter_textifier(
        self,
        item,
        drop_value_datatypes=('external-id', 'commonsMedia', 'url', 'geo-shape', 'tabular-data'),
        one_value_datatypes=('monolingualtext',),
        drop_claim_pids=(),
    ):

        def _normalize_claims(claims):
            claims = [
                claim for claim in claims
                if (claim.property.id not in drop_claim_pids) and (not self.filter(claim.property.id))
            ]
            claims = sorted(
                claims,
                key=lambda claim: (self.get_sort_index(claim.property.id), claim.property.id)
            )

            for claim in claims:
                if claim.datatype in drop_value_datatypes:
                    claim.values = []
                elif claim.datatype in one_value_datatypes:
                    claim.values = claim.values[:1]

                for value in claim.values:
                    if not value.qualifiers:
                        continue
                    value.qualifiers = _normalize_claims(value.qualifiers)

            return claims

        item.claims = _normalize_claims(item.claims)
        return item


class WikidataItemFilter:

    def __init__(self, lang, fallback_lang=None):
        self.lang = lang
        self.fallback_lang = fallback_lang or lang

    def has_label(self, item):
        has_lang = self.lang in item.get('labels', {})
        has_fallback_lang = self.fallback_lang and (self.fallback_lang in item.get('labels', {}))
        has_mul_lang = 'mul' in item.get('labels', {})

        return has_lang or has_fallback_lang or has_mul_lang

    def has_description(self, item):
        has_lang = self.lang in item.get('descriptions', {})
        has_fallback_lang = self.fallback_lang and (self.fallback_lang in item.get('descriptions', {}))
        has_mul_lang = 'mul' in item.get('descriptions', {})

        return has_lang or has_fallback_lang or has_mul_lang

    def has_sitelink(self, item):
        return any([sitelink.endswith('wiki') for sitelink in item.get('sitelinks', {})])

    def not_disambiguation(self, item, disambiguation_qid='Q4167410'):
        instanceof = item.get('claims', {}).get('P31', [])
        instanceof = [
            c.get('mainsnak', {}).get('datavalue', {}).get('value', {}).get('id')
            for c in instanceof
            if c.get('rank') != 'deprecated'
        ]
        return disambiguation_qid not in instanceof

    def has_content(self, item):
        return self.has_description(item) or len(item.get('claims', {})) > 0

    def filter(self, item):
        if item.get("id", "").startswith("P"):
            return self.has_label(item) and self.has_content(item)

        return self.has_label(item) \
            and self.has_content(item) \
                and self.not_disambiguation(item) \
                    and self.has_sitelink(item)

class WikidataScholarlyArticleFilter:

    instance_of_qids = ["Q13442814","Q7318358","Q2782326","Q815382","Q1348305","Q187685","Q1907875","Q18918145","Q1266946","Q23927052","Q1504425","Q45182324","Q1402850","Q7316896","Q580922","Q30749496","Q111475835","Q92998777","Q114613919","Q798134","Q1385450","Q10885494","Q51282918","Q51282711","Q111475860","Q51283092","Q15706459","Q59387148","Q110716513","Q58897583","Q51283145","Q54670950","Q91901000","Q111476177","Q51283053","Q1414362","Q51283181","Q51282999","Q51283199","Q82969330","Q112585758","Q118114827","Q106276531","Q1884156","Q51283362","Q46629343","Q100328456","Q51283219","Q70471362"]

    def __init__(self, lang, fallback_lang=None):
        self.lang = lang
        self.fallback_lang = fallback_lang or lang

    def has_label(self, item):
        has_lang = self.lang in item.get('labels', {})
        has_fallback_lang = self.fallback_lang and (self.fallback_lang in item.get('labels', {}))
        has_mul_lang = 'mul' in item.get('labels', {})

        return has_lang or has_fallback_lang or has_mul_lang

    def has_description(self, item):
        has_lang = self.lang in item.get('descriptions', {})
        has_fallback_lang = self.fallback_lang and (self.fallback_lang in item.get('descriptions', {}))
        has_mul_lang = 'mul' in item.get('descriptions', {})

        return has_lang or has_fallback_lang or has_mul_lang

    def not_disambiguation(self, item, disambiguation_qid='Q4167410'):
        instanceof = item.get('claims', {}).get('P31', [])
        instanceof = [
            c.get('mainsnak', {}).get('datavalue', {}).get('value', {}).get('id')
            for c in instanceof
            if c.get('rank') != 'deprecated'
        ]
        return disambiguation_qid not in instanceof

    def not_scholarly_article(self, item, publication_type_pid='P13046'):
        # Check if the item is an instance of any of the scholarly article QIDs
        instanceof = item.get('claims', {}).get('P31', [])
        instanceof = [
            c.get('mainsnak', {}).get('datavalue', {}).get('value', {}).get('id')
            for c in instanceof
            if c.get('rank') != 'deprecated'
        ]
        instanceof_scholarlyarticle = any(qid in self.instance_of_qids for qid in instanceof)

        # Check if it has a non-deprecated publication type of scholarly work (P13046) statement
        publication_type_claims = item.get('claims', {}).get(publication_type_pid, [])
        publication_type_claims = [c for c in publication_type_claims if c.get('rank') != 'deprecated']
        publication_type_claims = len(publication_type_claims) > 0

        return not (instanceof_scholarlyarticle or publication_type_claims)

    def has_content(self, item):
        return self.has_description(item) or len(item.get('claims', {})) > 0

    def filter(self, item):
        if item.get("id", "").startswith("P"):
            return self.has_label(item) and self.has_content(item)

        return self.has_label(item) \
            and self.has_content(item) \
                and self.not_disambiguation(item) \
                    and self.not_scholarly_article(item)
