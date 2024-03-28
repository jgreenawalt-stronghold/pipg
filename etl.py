import aiohttp
import asyncpg
from datetime import datetime
from decimal import Decimal
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
        self.pg_host = os.getenv("PG_CENTRAL_HOST")
        self.pg_database = os.getenv("PG_CENTRAL_DATABASE")
        self.pg_user = os.getenv("PG_CENTRAL_USER")
        self.pg_password = os.getenv("PG_CENTRAL_PASSWORD")

    async def pi(self, path, session):
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

    async def pg(self, query):
        pg = await asyncpg.connect (
            host = self.pg_host,
            database = self.pg_database,
            user = self.pg_user,
            password = self.pg_password,
        )
        try:
            pg_select = await pg.fetch(query)
            for value in pg_select:
                if isinstance(value[0], Decimal):
                    return float(value[0])
                else:
                    return(value[0])
        except (Exception, asyncpg.PostgresError) as e:
            print(e)

    async def pjm(self, url, headers, session):
        async with session.get(url, headers=headers) as response:
            try:
                if response.status == 200:
                    r = await response.json(content_type="application/octet-stream")
                else:
                    raise Exception
            except Exception as e:
                print(e)
            return r

class Transformer:
    def __init__(self):
        pass

    async def pjm_da_hrl_lmps(self, data):
        value = data[0]['total_lmp_da']
        return value

class Loader:

    def __init__(self):

        self.pi_headers = {"Content-type": "application/json", "X-Requested-With": "XmlHttpRequest"}
        self.pi_host = os.getenv("PI_HOST")
        self.pi_user = os.getenv("PI_USER")
        self.pi_password = os.getenv("PI_PASSWORD")
        self.pg_host = os.getenv("PG_CENTRAL_HOST")
        self.pg_database = os.getenv("PG_CENTRAL_DATABASE")
        self.pg_user = os.getenv("PG_CENTRAL_USER")
        self.pg_password = os.getenv("PG_CENTRAL_PASSWORD")

    async def pi(self, path, timestamp, data, session):
        try:
            async with session.get(
                path,
                auth=aiohttp.BasicAuth(self.pi_user, self.pi_password),
                ssl=False,
                headers=self.pi_headers,
            ) as response:
                if response.status == 200:
                    r = await response.json()
                    payload = {
                    "Timestamp": timestamp,
                    "Value": data
                    }
                    await session.post(
                        r["Links"]["Value"],
                        auth=aiohttp.BasicAuth(self.pi_user, self.pi_password),
                        ssl=False,
                        json=payload,
                        headers=self.pi_headers,
                    )
        except Exception as e:
            print(e)

    async def pg(self, schema, table, columns, data):
        pg = await asyncpg.connect (
            host = self.pg_host,
            database = self.pg_database,
            user = self.pg_user,
            password = self.pg_password,
        )

        fields = ', '.join(columns)
        placeholders = ', '.join(['$' + str(i+1) for i in range(len(columns))])
        pg_insert = f"INSERT INTO {schema}.{table} ({fields}) VALUES ({placeholders})"
        try:
            await pg.execute(pg_insert, *data)

        except (Exception, asyncpg.PostgresError) as e:
            print(e)