from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from assignment2.mqtt import subscriber
from assignment2.dashboard import app


class ReadySubscriber(subscriber.PriceSubscriber):
    def __init__(self, args):
        super().__init__(args)
        self.client.on_subscribe = self.on_subscription_ready

    def on_subscription_ready(self, client, userdata, mid, reason_codes, properties):
        if any(code.is_failure for code in reason_codes):
            print('Subscription rejected', file=sys.stderr, flush=True)
            self.stop_event.set()
        else:
            print('SUBSCRIBED: price receiver ready', flush=True)


if __name__ == '__main__':
    subscriber.PriceSubscriber = ReadySubscriber
    raise SystemExit(app.main())
