from elasticsearch import Elasticsearch
from py_stringmatching.similarity_measure.levenshtein import Levenshtein
from py_stringmatching.similarity_measure.jaro_winkler import JaroWinkler

from project_utilities.conitnuous_id_generator import ContinuousIDGenerator


class Matcher:
    def __init__(self):
        self.es_client = Elasticsearch()
        self.c_id_generator = ContinuousIDGenerator(prefix="CMatched")
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

    def match_cities_by_company_names(self, company_name: str):
        company_name_preprocessed = company_name\
            .replace("GmbH", "")\
            .replace("AG", "")\
            .replace("mbH", "")\
            .replace("KG", "")

        found_corporates_prefilter = list()
        query_body = {
            "query": {
                "match": {
                    "name": {
                        "query": company_name_preprocessed
                    }
                }
            }
        }
        prefilter_result = self.es_client.search(index="corporates-imported", body=query_body)
        found_corporates_prefilter += list(
            map(lambda x: x["_source"],
                prefilter_result["hits"]["hits"]))

        cities = list()
        lev_sim_calculator = Levenshtein()
        jaro_winkler_sim_calculator = JaroWinkler()
        for found_corporate in found_corporates_prefilter:
            if found_corporate["address"]["city"] == "":
                continue
            lev_sim = lev_sim_calculator.get_sim_score(found_corporate["name"], company_name)
            jaro_winkler_sim = jaro_winkler_sim_calculator.get_sim_score(found_corporate["name"], company_name)
            if lev_sim >= 0.8 and jaro_winkler_sim >= 0.7:
                cities.append(found_corporate["address"]["city"])

        return cities

    @staticmethod
    def get_known_companies_with_id(c_id, known_companies):
        for company_name in known_companies.keys():
            if c_id in known_companies[company_name]:
                return company_name

    @staticmethod
    def is_interesting_fold_out(fold_out):
        return fold_out["company_name"] != "" and fold_out["continuous_id"].startswith("lr")

    def persist_localized_company(self, company_name, c_id, s_id, s_prefix, relation, cities):
        for city in cities:
            new_c_id = self.c_id_generator.get_next_identifier()
            self.es_client.index(index="company_localized", body={
                "c_id": new_c_id,
                "oc_id": c_id,
                "company_name": company_name,
                "source_id": s_id,
                "source_prefix": s_prefix,
                "relation": relation,
                "city": city
            }, id=new_c_id)

    def match(self):
        interesting_fold_outs = 0
        fold_outs_lost_due_missing_city = 0
        for fold_out in self.iterate_over_all_fold_outs():
            if Matcher.is_interesting_fold_out(fold_out):
                interesting_fold_outs += 1
                company_name = fold_out["company_name"]
                cities = self.match_cities_by_company_names(company_name)
                if len(cities) == 0:
                    fold_outs_lost_due_missing_city += 1
                    continue
                self.persist_localized_company(
                    company_name=company_name,
                    c_id=fold_out["continuous_id"],
                    s_id=fold_out["source_id"],
                    s_prefix=fold_out["source_prefix"],
                    cities=cities,
                    relation=fold_out["relation"]
                )
        print(f"All: {interesting_fold_outs}, Lost: {fold_outs_lost_due_missing_city}")
