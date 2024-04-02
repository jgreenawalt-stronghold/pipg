import aiohttp
import asyncio
import logging
import aioschedule
from database import query
from interface import pjm
from etl import extractor, transformer, loader
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

async def main():
	extract = extractor.Extractor()
	transform = transformer.Transformer()
	load = loader.Loader()
	pi_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
	async with aiohttp.ClientSession() as session:
		tag = "JMLT301" # ASH SILO LEVEL INDICATION
		columns = ["timestamp", "silo_level"]

		data = await extract.pi(f"{extract.pi_host}{tag}", session)
		aioschedule.every(10).minutes.do(load.pg("power", "sg_ash_silo", columns, data))

		hashprice = await extract.pg(query.hashprice)
		sg_hashrate = await extract.pg(query.sg_hashrate)
		sg_foundry_jv_hashrate = await extract.pg(query.sg_foundry_jv_hashrate)
		
		sg_hashrate_tag = "BTC_SGTotalHashRate"
		sg_foundry_jv_hashrate_tag = "BTC_FoundryJVHashrate"
		hashprice_tag = "BTC_MiningProfitability"

		await load.pi(f"{load.pi_host}{hashprice_tag}", pi_time, hashprice, session)
		await load.pi(f"{load.pi_host}{sg_hashrate_tag}", pi_time, sg_hashrate, session)
		await load.pi(f"{load.pi_host}{sg_foundry_jv_hashrate_tag}", pi_time, sg_foundry_jv_hashrate, session)

		e_pjm_da_hrl_lmp = await extract.pjm(pjm.sg_da_hrl_lmp_url, pjm.headers, session)
		t_pjm_da_hrl_lmp = await transform.pjm_da_hrl_lmps(e_pjm_da_hrl_lmp)
		await load.pi(f"{load.pi_host}{pjm.sg_da_hrl_lmp_tag}", pi_time, t_pjm_da_hrl_lmp, session)

		e_penelec_da_hrl_lmp = await extract.pjm(pjm.penelec_da_hrl_lmp_url, pjm.headers, session)
		t_penelec_da_hrl_lmp = await transform.pjm_da_hrl_lmps(e_penelec_da_hrl_lmp)
		await load.pi(f"{load.pi_host}{pjm.penelec_da_hrl_lmp_tag}", pi_time, t_penelec_da_hrl_lmp, session)

if __name__ == "__main__":
	loop = asyncio.get_event_loop()
	loop.run_until_complete(main())
