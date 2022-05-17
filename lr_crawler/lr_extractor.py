import logging

import requests

from lr_producer import LrProducer
from build.gen.bakdata.lobbyist.v1.lobbyist_pb2 import Lobbyist, Related_Person

log = logging.getLogger(__name__)


class LrExtractor:
    def __init__(self, organization_name):
        self.organization_name = organization_name
        self.producer = LrProducer()

    def extract(self):
        log.info(f"Sending request for: {self.organization_name}")
        detailed_data = self.request_detailed_data()
        for interesting_lobbyist in self.filter_interesting_lobbyists(detailed_data):
            lobbyist = Lobbyist()
            try:
                lobbyist.lobbyist_name = LrExtractor.extract_lobbyist_name_from_lobbyist(interesting_lobbyist)
            except KeyError:
                continue
            lobbyist.client_name = self.organization_name
            lobbyist.fields_of_interests.extend(LrExtractor.extract_lobbies_from_lobbyist(interesting_lobbyist))
            for person in LrExtractor.extract_related_persons_from_lobbyist(interesting_lobbyist):
                related_person = Related_Person()
                related_person.first_name = person["first_name"]
                related_person.last_name = person["last_name"]
                lobbyist.related_persons.extend([related_person])
            log.debug(lobbyist)
            self.producer.produce_to_topic(lobbyist)

    def request_detailed_data(self) -> str:
        url = \
            f"https://www.lobbyregister.bundestag.de/sucheDetailJson?q={self.organization_name}&sort=REGISTRATION_DESC"
        return requests.get(url=url).json()

    @staticmethod
    def extract_lobbyists_from_response(json_response):
        return list(map(lambda detailed_entry: detailed_entry["registerEntryDetail"], json_response["results"]))

    @staticmethod
    def extract_lobbies_from_lobbyist(lobbyist):
        return list(map(lambda field_of_interest: field_of_interest["de"], lobbyist["fieldsOfInterest"]))

    @staticmethod
    def extract_lobbyist_name_from_lobbyist(lobbyist):
        return lobbyist["lobbyistIdentity"]["name"]

    @staticmethod
    def extract_client_names_from_lobbyist(lobbyist):
        return list(map(lambda client_organization: client_organization["name"], lobbyist["clientOrganizations"]))

    @staticmethod
    def extract_name_information_from_person(person):
        return {"first_name": person["commonFirstName"],
                "last_name": person["lastName"]}

    @staticmethod
    def extract_related_persons_from_lobbyist(lobbyist):
        return list(map(LrExtractor.extract_name_information_from_person,
                        lobbyist["lobbyistIdentity"]["legalRepresentatives"])) + list(map(
            LrExtractor.extract_name_information_from_person, lobbyist["lobbyistIdentity"]["namedEmployees"]))

    def filter_interesting_lobbyists(self, detailed_data_json_response):
        interesting_lobbyists = list()
        for lobbyist in LrExtractor.extract_lobbyists_from_response(detailed_data_json_response):
            client_names = LrExtractor.extract_client_names_from_lobbyist(lobbyist)
            if self.organization_name in client_names:
                interesting_lobbyists.append(lobbyist)
        return interesting_lobbyists
