from elasticsearch import Elasticsearch
from functools import lru_cache


class Matcher:
    # copied from https://towardsdatascience.com/text-similarity-w-levenshtein-distance-in-python-2f7478986e75
    @staticmethod
    def lev_dist(string_a, string_b):
        @lru_cache(None)  # for memorization
        def min_dist(s1, s2):

            if s1 == len(string_a) or s2 == len(string_b):
                return len(string_a) - s1 + len(string_b) - s2

            # no change required
            if string_a[s1] == string_b[s2]:
                return min_dist(s1 + 1, s2 + 1)

            return 1 + min(
                min_dist(s1, s2 + 1),  # insert character
                min_dist(s1 + 1, s2),  # delete character
                min_dist(s1 + 1, s2 + 1),  # replace character
            )

        return min_dist(0, 0)

    def __init__(self):
        self.es_client = Elasticsearch()
        self.companies = dict()
        self.bloom_filter = list()

    # method inspired by
    # https://techoverflow.net/2019/05/03/elasticsearch-how-to-iterate-all-documents-in-index-using-python-up-to-10000-documents/
    def iterate_over_all_fold_outs(self):
        offset = 0
        pagesize = 5
        while True:
            result = self.es_client.search(index="fold_out-events", body={
                "size": pagesize,
                "from": offset
            })
            hits = result["hits"]["hits"]
            # Stop after no more docs
            if not hits:
                break
            # Yield each entry
            yield from (hit['_source'] for hit in hits)
            # Continue from there
            offset += pagesize

    def match_company_names(self, company_name: str):
        query_body = {
            "query": {
                "match": {
                    "company_name": {
                        "query": company_name.replace("GmbH", ""),
                        "operator": "AND"
                    }
                }
            }
        }
        prefilter = self.es_client.search(index="fold_out-events", body=query_body)
        potential_object = list(
            map(lambda x: x["_source"],
                prefilter["hits"]["hits"]))
        result = list()
        for potential in potential_object:
            b_company_name = potential["company_name"]
            if b_company_name != "":
                if Matcher.lev_dist(company_name, b_company_name) < 20:
                    result.append(potential["continuous_id"])
        return result

    @staticmethod
    def get_known_companies_with_id(c_id, known_companies):
        for company_name in known_companies.keys():
            if c_id in known_companies[company_name]:
                return company_name

    def match(self):
        for fold_out in self.iterate_over_all_fold_outs():
            if fold_out["company_name"] != "":
                matched_c_ids = self.match_company_names(fold_out["company_name"])
                # matched_c_ids = list(
                #     map(lambda x: x["_source"]["continuous_id"],
                #         self.match_company_names(fold_out["company_name"].replace("GmbH", ""))["hits"]["hits"]))
                already_seen = list()
                for matched_id in matched_c_ids:
                    if matched_id in self.bloom_filter:
                        already_seen.append(matched_id)
                if len(already_seen) > 0:
                    associated_companies = list()
                    for matched_id in already_seen:
                        associated_companies.append(Matcher.get_known_companies_with_id(matched_id, self.companies))
                    resulting_c_ids = list()
                    for company in set(associated_companies):
                        resulting_c_ids = resulting_c_ids + self.companies[company]
                        self.companies.pop(company)
                    self.companies[fold_out["company_name"]] = list(set(resulting_c_ids + matched_c_ids))
                else:
                    self.companies[fold_out["company_name"]] = list(set(matched_c_ids))
                self.bloom_filter = self.bloom_filter + matched_c_ids
        finish = dict()
        for company in self.companies.keys():
            names = list()
            has_rb = False
            for c_id in self.companies[company]:
                if c_id[0:2] == "rb":
                    has_rb = True
                names.append(self.es_client.get(index="fold_out-events", id=c_id)["_source"]["company_name"])
            finish[company] = names
            if len(names) > 1 and has_rb:
                print(f"{company}: {' '.join(names)}")

