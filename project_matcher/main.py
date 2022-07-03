from project_matcher.matcher import Matcher

import logging
import os

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "CRITICAL"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)

if __name__ == "__main__":
    sample_matcher = Matcher()
    sample_matcher.delete_company_localized_index()
    sample_matcher.enrich_with_company_location()
    sample_matcher.clean_up_localized_companies()
    sample_matcher.export_to_csv()
