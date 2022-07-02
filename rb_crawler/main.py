import logging
import os
import random

import click

from rb_crawler.state import State, states
from rb_crawler.rb_extractor import RbExtractor

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)


@click.command()
@click.option("-i", "--id", "rb_id", type=int, help="The rb_id to initialize the crawl from")
@click.option("-s", "--state", type=click.Choice(State), help="The state ISO code")
def run(rb_id: int, state: State):
    if state == State.SCHLESWIG_HOLSTEIN:
        if rb_id < 7830:
            error = ValueError("The start rb_id for the state SCHLESWIG_HOLSTEIN (sh) is 7831")
            log.error(error)
            exit(1)
    RbExtractor(rb_id, state.value).extract(should_loop=True)


def run_expo():
    for _ in range(0, 100):
        for state in states:
            RbExtractor(random.randint(8000, 100000), state).extract(should_loop=False)


if __name__ == "__main__":
    run_expo()
