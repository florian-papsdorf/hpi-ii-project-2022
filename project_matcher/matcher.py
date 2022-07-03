import logging
import time
import csv

from elasticsearch import Elasticsearch
from py_stringmatching.similarity_measure.jaro_winkler import JaroWinkler
from py_stringmatching.similarity_measure.levenshtein import Levenshtein

from project_utilities.conitnuous_id_generator import ContinuousIDGenerator

log = logging.getLogger(__name__)


class Matcher:
    COMPANY_LOCALIZED_INDEX_NAME = "company_localized"

    def __init__(self):
        self.es_client = Elasticsearch()
        self.c_id_generator = ContinuousIDGenerator(prefix="CMatched")
        self.companies = dict()
        self.bloom_filter = list()
        self.lev_sim_calculator = Levenshtein()
        self.jaro_winkler_sim_calculator = JaroWinkler()

    def delete_company_localized_index(self):
        self.es_client.indices.delete(index=Matcher.COMPANY_LOCALIZED_INDEX_NAME)

    # method inspired by
    # https://techoverflow.net/2019/05/03/elasticsearch-how-to-iterate-all-documents-in-index-using-python-up-to-10000-documents/
    def iterate_over_index(self, index_name):
        offset = 0
        pagesize = 5
        while True:
            result = self.es_client.search(index=index_name, body={
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

    def iterate_over_all_fold_outs(self):
        return self.iterate_over_index("fold_out-events")

    def iterate_over_all_localized_companies(self):
        return self.iterate_over_index(Matcher.COMPANY_LOCALIZED_INDEX_NAME)

    def match_cities_by_company_names(self, company_name: str):
        company_name_preprocessed = company_name \
            .replace("GmbH", "") \
            .replace("AG", "") \
            .replace("mbH", "") \
            .replace("KG", "")

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
        found_corporates_prefilter = list(
            map(lambda x: x["_source"],
                prefilter_result["hits"]["hits"]))

        cities = list()
        for found_corporate in found_corporates_prefilter:
            if found_corporate["address"]["city"] == "":
                continue
            lev_sim = self.lev_sim_calculator.get_sim_score(found_corporate["name"], company_name)
            jaro_winkler_sim = self.jaro_winkler_sim_calculator.get_sim_score(found_corporate["name"], company_name)
            if lev_sim >= 0.8 and jaro_winkler_sim >= 0.7:
                cities.append(found_corporate["address"]["city"])

        return cities

    def match_duplicates_in_city(self, localized_company):
        query_body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "city": localized_company["city"]
                            }
                        },
                        {
                            "match": {
                                "relation": localized_company["relation"]
                            }
                        }
                    ]
                }
            }
        }
        prefilter_result = self.es_client.search(index=Matcher.COMPANY_LOCALIZED_INDEX_NAME, body=query_body)
        found_corporates_prefilter = list(
            map(lambda x: x["_source"],
                prefilter_result["hits"]["hits"]))

        found_corporates_final = list()

        for found_corporate in found_corporates_prefilter:
            if found_corporate["city"] != localized_company["city"] or \
                    found_corporate["relation"] != localized_company["relation"]:
                continue
            lev_sim = self.lev_sim_calculator.get_sim_score(found_corporate["company_name"],
                                                            localized_company["company_name"])
            jaro_winkler_sim = self.jaro_winkler_sim_calculator.get_sim_score(found_corporate["company_name"],
                                                                              localized_company["company_name"])
            if lev_sim > 0.95 and jaro_winkler_sim > 0.95 and found_corporate["c_id"] != localized_company["c_id"]:
                found_corporates_final.append(found_corporate["c_id"])

        return found_corporates_final

    def clean_up_localized_companies(self):
        log.critical("Start clean up")
        to_delete = list()
        start_time = time.time()
        for company in self.iterate_over_all_localized_companies():
            intermediate = self.match_duplicates_in_city(company)
            if len(intermediate) > 0:
                to_delete.append(intermediate)
        log.critical(f"Duplicates in localized companies: {len(to_delete)}")
        for entry in to_delete:
            for c_id in entry:
                try:
                    self.es_client.delete(index=Matcher.COMPANY_LOCALIZED_INDEX_NAME, id=c_id)
                except:
                    continue
        end_time = time.time()
        log.critical(f"Time consumed (s): {end_time - start_time}")

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
            self.es_client.index(index=Matcher.COMPANY_LOCALIZED_INDEX_NAME, body={
                "c_id": new_c_id,
                "oc_id": c_id,
                "company_name": company_name,
                "source_id": s_id,
                "source_prefix": s_prefix,
                "relation": relation,
                "city": city
            }, id=new_c_id)

    def enrich_with_company_location(self):
        log.critical("Start Enrichment")
        start_time = time.time()
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
        end_time = time.time()
        log.critical(
            f"All fold outs: {interesting_fold_outs}, Lost fold outs due to missing city: {fold_outs_lost_due_missing_city}")
        log.critical(f"Time consumed: {end_time - start_time}")

    def export_to_csv(self):
        with open("localized_lobbyists_clients.csv", "w") as file_handler:
            csv_writer = csv.writer(file_handler)
            csv_writer.writerow(["c_id", "company_name", "city", "relation"])
            for localized_org in self.iterate_over_all_localized_companies():
                csv_writer.writerow([
                    localized_org["c_id"],
                    localized_org["company_name"],
                    localized_org["city"],
                    localized_org["relation"]
                ])
