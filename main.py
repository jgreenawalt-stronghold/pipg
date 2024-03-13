import aiohttp
import asyncio
from database import query
from etl import Extractor, Loader
from dotenv import load_dotenv

load_dotenv()

async def main():
	extract = Extractor()
	load = Loader()
	async with aiohttp.ClientSession() as session:
		tag = "JMLT301" # ASH SILO LEVEL INDICATION
		columns = ["timestamp", "silo_level"]

		data = await extract.pi(f"{extract.pi_host}{tag}", session)
		await load.pg("power", "sg_ash_silo", columns, data)

		hashprice = await extract.pg(query.hashprice)
		sg_hashrate = await extract.pg(query.sg_hashrate)
		sg_foundry_jv_hashrate = await extract.pg(query.sg_foundry_jv_hashrate)
		sg_hashrate_tag = "BTC_SGTotalHashRate"
		sg_foundry_jv_hashrate_tag = "BTC_FoundryJVHashrate"
		hashprice_tag = "BTC_MiningProfitability"
		await load.pi(f"{load.pi_host}{hashprice_tag}", hashprice, session)
		await load.pi(f"{load.pi_host}{sg_hashrate_tag}", sg_hashrate, session)
		await load.pi(f"{load.pi_host}{sg_foundry_jv_hashrate_tag}", sg_foundry_jv_hashrate, session)

if __name__ == "__main__":
	loop = asyncio.get_event_loop()
	loop.run_until_complete(main())
