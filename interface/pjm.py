import os
import urllib.parse
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("PJM_API_KEY")
pc_pnode_id = 50746
pc_zone = "METED"

headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    "ACCEPT-ENCODING": "gzip, deflate, br",
    "Ocp-Apim-Subscription-Key": api_key
}

parameters = urllib.parse.urlencode(
    {
        "download": True,
        "startRow": 1,
        "datetime_beginning_utc": 'CurrentHour'
    }
)
tag = {
        "meted_da_hrl_lmp": "PJMPriceMetEd",
        "pc_da_hrl_lmp": "PJMPriceDA",
        "pc_rt_lmp": "PJMPriceRT",
        "pc_rt_lmp_marginal_loss": "PJMPriceML",
        "pc_rt_lmp_congestion": "PJMPriceCongestion"
}

url = {
        "pc_lmp_rt":f"https://api.pjm.com/api/v1/rt_unverified_fivemin_lmps?pnode_id={pc_pnode_id}&{parameters}",
        "pc_da_hrl_lmp": f"https://api.pjm.com/api/v1/da_hrl_lmps?pnode_id={pc_pnode_id}&{parameters}",
        "meted_da_hrl_lmp": f"https://api.pjm.com/api/v1/da_hrl_lmps?zone={pc_zone}&{parameters}"
}
