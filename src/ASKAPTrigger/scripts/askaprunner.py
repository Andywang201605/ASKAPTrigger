#!/usr/bin/python

from ASKAPTrigger.askaptrigger import LotrunRunner, SBStateSubscriber

def main():
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(description='Setup ASKAP Schedblock Listener', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("-p", "--project", type=str, help="alias name for sbid mwa project pairs", default="default")
    parser.add_argument("--dryrun", action="store_true", help="whether run as a dry run or not", default=False)
    values = parser.parse_args()

    runner = LotrunRunner(values=values)
    state = SBStateSubscriber(runner)
    try:
        state.ice.waitForShutdown()
    except KeyboardInterrupt:
        state.topic.unsubscribe(state.subscriber)
        state.ice.shutdown()