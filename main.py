import aiohttp
import asyncio
from database import query
from interface import pjm, btc
from etl import Extractor, Transformer, Loader
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

async def main():
	extract = Extractor()
	transform = Transformer()
	load = Loader()
	pi_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
	async with aiohttp.ClientSession() as session:
		# Extract jobs
		pc_hashrate = await extract.pg(query.pc_hashrate)
		e_pjm_rt_lmp = await extract.pjm(pjm.url['pc_lmp_rt'], pjm.headers, session)
		e_meted_da_hrl_lmp = await extract.pjm(pjm.url['meted_da_hrl_lmp'], pjm.headers, session)
		e_pjm_da_hrl_lmp = await extract.pjm(pjm.url['pc_da_hrl_lmp'], pjm.headers, session)

		# Transform jobs	
		t_pjm_da_hrl_lmp = await transform.pjm_da_hrl_lmps(e_pjm_da_hrl_lmp)
		t_meted_da_hrl_lmp = await transform.pjm_da_hrl_lmps(e_meted_da_hrl_lmp)
		t_pjm_rt_lmp_total = await transform.pjm_total_lmp_rt(e_pjm_rt_lmp)
		t_pjm_congestion_price_rt = await transform.pjm_congestion_price_rt(e_pjm_rt_lmp)
		t_pjm_marginal_loss_price_rt = await transform.pjm_marginal_loss_price_rt(e_pjm_rt_lmp)
		
		# Load jobs		
		await load.pi(f"{load.pi_host}{pjm.tag['pc_rt_lmp']}", pi_time, t_pjm_da_hrl_lmp, session)
		await load.pi(f"{load.pi_host}{pjm.tag['pc_rt_lmp_congestion']}", pi_time, t_pjm_congestion_price_rt, session)
		await load.pi(f"{load.pi_host}{pjm.tag['pc_rt_lmp_marginal_loss']}", pi_time, t_pjm_marginal_loss_price_rt, session)
		await load.pi(f"{load.pi_host}{pjm.tag['pc_da_hrl_lmp']}", pi_time, t_pjm_da_hrl_lmp, session)
		await load.pi(f"{load.pi_host}{pjm.tag['meted_da_hrl_lmp']}", pi_time, t_meted_da_hrl_lmp, session)
		await load.pi(f"{load.pi_host}{btc.tag['pc_hashrate']}", pi_time, pc_hashrate, session)

if __name__ == "__main__":
	loop = asyncio.get_event_loop()
	loop.run_until_complete(main())
