import aiohttp
import asyncpg
from datetime import datetime
import pytz
import urllib.parse
import urllib3
import os

class Extractor:

    def __init__(self):
        
        self.pi_headers = {"Content-type": "application/json", "X-Requested-With": "XmlHttpRequest"}
        self.pi_host = os.getenv("PI_HOST")
        self.pi_user = os.getenv("PI_USER")
        self.pi_password = os.getenv("PI_PASSWORD")

    async def get_pi_value(self, path, session):
        try:
            async with session.get(path,auth=aiohttp.BasicAuth(self.pi_user, self.pi_password),ssl=False,headers=self.pi_headers) as response:
                if response.status == 200:
                    r = await response.json()
                    get_value = await (await session.get(r["Links"]["Value"],auth=aiohttp.BasicAuth(self.pi_user, self.pi_password),ssl=False,headers=self.pi_headers)).json()
                    timestamp = datetime.utcnow()
                    value = get_value["Value"]
                    data = (timestamp, value)                
                    return data
        
        except Exception as e:
                    print(e)

class Loader:

    def __init__(self):

        self.host = os.getenv("PG_CENTRAL_HOST")
        self.database = os.getenv("PG_CENTRAL_DATABASE")
        self.user = os.getenv("PG_CENTRAL_USER")
        self.password = os.getenv("PG_CENTRAL_PASSWORD")

    async def load_pg(self, schema, table, columns, data):
        conn = await asyncpg.connect (
            host = self.host,
            database = self.database,
            user = self.user,
            password = self.password,
        )

        fields = ', '.join(columns)
        placeholders = ', '.join(['$' + str(i+1) for i in range(len(columns))])
        insert_pg = f"INSERT INTO {schema}.{table} ({fields}) VALUES ({placeholders})"
        try:
            await conn.execute(insert_pg, *data)

        except (Exception, asyncpg.PostgresError) as e:
            print(e)