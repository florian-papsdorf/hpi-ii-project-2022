import logging

import requests

from lr_producer import LrProducer
from build.gen.bakdata.lobbyist.v2.lobbyist_pb2 import Lobbyist
from project_utilities.fold_out_producer import FoldOutProducer
from project_utilities.conitnuous_id_generator import ContinuousIDGenerator
from project_utilities.constant import LR_PREFIX

log = logging.getLogger(__name__)


class LrExtractor:
    def __init__(self):
        self.producer = LrProducer()
        self.fold_out_id_generator = ContinuousIDGenerator(LR_PREFIX)
        self.fold_out_producer = FoldOutProducer()

    def extract(self):
        detailed_data = LrExtractor.request_detailed_data()
        for interesting_lobbyist in self.extract_lobbyists_from_response(detailed_data):
            lobbyist = Lobbyist()
            lobbyist.lobbyist_id = LrExtractor.extract_lobbyist_id_from_lobbyist(interesting_lobbyist)
            lobbyist.lobbyist_name = LrExtractor.extract_lobbyist_name_from_lobbyist(interesting_lobbyist)
            lobbyist.organization_client_names.extend(
                LrExtractor.extract_organization_client_names_from_lobbyist(interesting_lobbyist))
            lobbyist.fields_of_interests.extend(LrExtractor.extract_lobbies_from_lobbyist(interesting_lobbyist))
            for person in LrExtractor.extract_related_persons_from_lobbyist(interesting_lobbyist):
                related_person = lobbyist.related_persons.add()
                related_person.first_name = person["first_name"]
                related_person.last_name = person["last_name"]
            for person_client in LrExtractor.extract_person_client_names_from_lobbyist(interesting_lobbyist):
                person_client_element = lobbyist.person_client_names.add()
                person_client_element.first_name = person_client["first_name"]
                person_client_element.last_name = person_client["last_name"]
            lobbyist.donators.extend(LrExtractor.extract_donator_names_from_lobbyist(interesting_lobbyist))
            self.producer.produce_to_topic(lobbyist)
            # TODO: handle fold out
            # self.fold_out_producer.produce_to_topic(message)

    @staticmethod
    def request_detailed_data() -> str:
        url = \
            f"https://www.lobbyregister.bundestag.de/sucheDetailJson?sort=REGISTRATION_DESC"
        return requests.get(url=url).json()

    @staticmethod
    def extract_lobbyists_from_response(json_response):
        return list(map(lambda detailed_entry: detailed_entry["registerEntryDetail"], json_response["results"]))

    @staticmethod
    def extract_lobbyist_id_from_lobbyist(lobbyist):
        return str(lobbyist["id"])

    @staticmethod
    def extract_lobbies_from_lobbyist(lobbyist):
        return list(map(lambda field_of_interest: field_of_interest["code"], lobbyist["fieldsOfInterest"]))

    @staticmethod
    def extract_lobbyist_name_from_lobbyist(lobbyist):
        if lobbyist["lobbyistIdentity"]["identity"] == "NATURAL":
            return "{0} {1}".format(lobbyist["lobbyistIdentity"]["commonFirstName"],
                                    lobbyist["lobbyistIdentity"]["lastName"])
        return lobbyist["lobbyistIdentity"]["name"]

    @staticmethod
    def extract_organization_client_names_from_lobbyist(lobbyist):
        return list(
            map(lambda client_organization: client_organization["name"], lobbyist["clientOrganizations"]))

    @staticmethod
    def extract_person_client_names_from_lobbyist(lobbyist):
        return list(map(LrExtractor.extract_name_information_from_person, lobbyist["clientPersons"]))

    @staticmethod
    def extract_name_information_from_person(person):
        return {"first_name": person["commonFirstName"],
                "last_name": person["lastName"]}

    @staticmethod
    def extract_donator_names_from_lobbyist(lobbyist):
        # one donator could be enumerated multiple times due to different contribution fields
        return list(set(list(map(lambda donator: donator["name"], lobbyist["donators"]))))

    @staticmethod
    def extract_related_persons_from_lobbyist(lobbyist):
        if lobbyist["lobbyistIdentity"]["identity"] == "NATURAL":
            return [LrExtractor.extract_name_information_from_person(lobbyist["lobbyistIdentity"])]
        return list(map(LrExtractor.extract_name_information_from_person,
                        lobbyist["lobbyistIdentity"]["legalRepresentatives"])) + \
               list(map(LrExtractor.extract_name_information_from_person,
                        lobbyist["lobbyistIdentity"]["namedEmployees"]))
