import logging
import os

from lr_extractor import LrExtractor

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)

if __name__ == "__main__":
    sample_extractor = LrExtractor("SAP SE")
    sample_extractor.extract()
