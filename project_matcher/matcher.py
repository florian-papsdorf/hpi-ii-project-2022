from elasticsearch import Elasticsearch


class Matcher:
    def __init__(self):
        self.es_client = Elasticsearch()

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

    def match(self):
        for hit in self.iterate_over_all_fold_outs():
            if hit["company_name"] != "":
                pass
            if hit["person_name"] != "":
                pass
