#!/usr/bin/python

from ASKAPTrigger.askaptrigger import LotrunRunner, SBStateSubscriber

def main():
    runner = LotrunRunner()
    state = SBStateSubscriber(runner)
    try:
        state.ice.waitForShutdown()
    except KeyboardInterrupt:
        state.topic.unsubscribe(state.subscriber)
        state.ice.shutdown()