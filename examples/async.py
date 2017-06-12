# Hack to get relative imports - probably need to fix the dir structure instead but we need this at the minute for
# pytest to work
import os, sys, inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

print(parentdir)

from modules.poloniex.app import AsyncApp

__author__ = 'andrew.shvv@gmail.com'

class App(AsyncApp):
    def ticker(self, **kwargs):
        self.logger.info(kwargs)

    def trades(self, **kwargs):
        self.logger.info(kwargs)

    async def main(self):
        self.push.subscribe(topic="BTC_ETH", handler=self.trades)
        self.push.subscribe(topic="ticker", handler=self.ticker)
        volume = await self.public.return24hVolume()

        self.logger.info(volume)


app = App()
app.run()
