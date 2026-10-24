import asyncio
import logging
from amqtt.broker import Broker


async def main():
    broker = Broker({
        "listeners": {"default": {"type": "tcp", "bind": "127.0.0.1:1883"}},
        "plugins": {"amqtt.plugins.authentication.AnonymousAuthPlugin": {"allow_anonymous": True}},
    })
    await broker.start()
    print("MQTT broker ready on 127.0.0.1:1883", flush=True)
    try:
        await asyncio.Event().wait()
    finally:
        await broker.shutdown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
