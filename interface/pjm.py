import os
import urllib.parse
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("PJM_API_KEY")

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
penelec_da_hrl_lmp_tag = "pjmPenelecda"
sg_da_hrl_lmp_tag = "PJMPriceDA"
sg_pnode_id = 50789
sg_da_hrl_lmp_url = f"https://api.pjm.com/api/v1/da_hrl_lmps?pnode_id={sg_pnode_id}&{parameters}"
penelec_da_hrl_lmp_url = f"https://api.pjm.com/api/v1/da_hrl_lmps?zone=PENELEC&{parameters}"
