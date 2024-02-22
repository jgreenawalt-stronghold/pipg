import aiohttp
import asyncio
from etl import Extractor, Loader
from dotenv import load_dotenv

load_dotenv()

async def main():
	async with aiohttp.ClientSession() as session:
		tag = "JMLT301" # ASH SILO LEVEL INDICATION
		extract = Extractor()
		load = Loader()
		columns = ["timestamp", "silo_level"]

		data = await extract.get_pi_value(f"{extract.pi_host}{tag}", session)
		await load.load_pg("power", "sg_ash_silo", columns, data)

if __name__ == "__main__":
	loop = asyncio.get_event_loop()
	loop.run_until_complete(main())