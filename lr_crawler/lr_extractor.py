import logging

import requests

from lr_producer import LrProducer
from build.gen.bakdata.lobbyist.v1.lobbyist_pb2 import Lobbyist

log = logging.getLogger(__name__)


class LrExtractor:
    def __init__(self):
        self.producer = LrProducer()

    def extract(self):
        detailed_data = LrExtractor.request_detailed_data()
        for interesting_lobbyist in self.extract_lobbyists_from_response(detailed_data):
            lobbyist = Lobbyist()
            # TODO: handle self-employed lobbyists
            try:
                lobbyist.lobbyist_name = LrExtractor.extract_lobbyist_name_from_lobbyist(interesting_lobbyist)
            except KeyError:
                continue
            # TODO: extract client names
            lobbyist.client_name = ""
            lobbyist.fields_of_interests.extend(LrExtractor.extract_lobbies_from_lobbyist(interesting_lobbyist))
            for person in LrExtractor.extract_related_persons_from_lobbyist(interesting_lobbyist):
                related_person = lobbyist.related_persons.add()
                related_person.first_name = person["first_name"]
                related_person.last_name = person["last_name"]
                lobbyist.related_persons.extend([related_person])
            log.debug(lobbyist)
            self.producer.produce_to_topic(lobbyist)

    @staticmethod
    def request_detailed_data() -> str:
        url = \
            f"https://www.lobbyregister.bundestag.de/sucheDetailJson?sort=REGISTRATION_DESC"
        return requests.get(url=url).json()

    @staticmethod
    def extract_lobbyists_from_response(json_response):
        return list(map(lambda detailed_entry: detailed_entry["registerEntryDetail"], json_response["results"]))

    @staticmethod
    def extract_lobbies_from_lobbyist(lobbyist):
        return list(map(lambda field_of_interest: field_of_interest["code"], lobbyist["fieldsOfInterest"]))

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
