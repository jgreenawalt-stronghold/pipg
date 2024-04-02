import aiohttp
import asyncpg
from datetime import datetime
from decimal import Decimal
import pytz
import urllib.parse
import urllib3
import os
import logging

logging.basicConfig(filename='pipg.log', level=logging.INFO)

class ETL:
    def __init__(self):
        self.pi_headers = {"Content-type": "application/json", "X-Requested-With": "XmlHttpRequest"}
        self.pi_host = os.getenv("PI_HOST")
        self.pi_user = os.getenv("PI_USER")
        self.pi_password = os.getenv("PI_PASSWORD")
        self.pg_host = os.getenv("PG_CENTRAL_HOST")
        self.pg_database = os.getenv("PG_CENTRAL_DATABASE")
        self.pg_user = os.getenv("PG_CENTRAL_USER")
        self.pg_password = os.getenv("PG_CENTRAL_PASSWORD")

class Extractor(ETL):
    def __init__(self):
        super().__init__()

    async def pi(self, path, session):
        try:
            async with session.get(path,auth=aiohttp.BasicAuth(self.pi_user, self.pi_password),ssl=False,headers=self.pi_headers) as response:
                try:
                    if response.status == 200:
                        r = await response.json()
                        get_value = await (await session.get(r["Links"]["Value"],auth=aiohttp.BasicAuth(self.pi_user, self.pi_password),ssl=False,headers=self.pi_headers)).json()
                        timestamp = datetime.utcnow()
                        value = get_value["Value"]
                        data = (timestamp, value)                
                        return data
                    else:
                        logging.error(f"Failed to get Pi value: Unexpected response status code: {reponse.status}")
                except Exception as e:
                    logging.error(f"Failed to get Pi value: {e}")
        except Exception as e:
            logging.error(f"Failed to connect to Pi database: {e}")

    async def pg(self, query):
        try:
            async with await asyncpg.connect (
                host = self.pg_host,
                database = self.pg_database,
                user = self.pg_user,
                password = self.pg_password,
            ) as pg:
                try:
                    pg_select = await pg.fetch(query)
                    for value in pg_select:
                        if isinstance(value[0], Decimal):
                            return float(value[0])
                        else:
                            return(value[0])
                except (Exception, asyncpg.PostgresError) as e:
                    logging.error(f"Failed to execute Postgres query: {e}")
        except (Exception, asyncpg.PostgresError) as e:
            logging.error(f"Failed to connecto Postgres database: {e}")

    async def pjm(self, url, headers, session):
        try:
            async with session.get(url, headers=headers) as response:
                try:
                    if response.status == 200:
                        r = await response.json(content_type="application/octet-stream")
                        return r
                    else:
                        logging.error(f"Failed to get PJM data: Unexpected response status code {response.status}")
                except Exception as e:
                    logging.error(f"Failed to parse PJM data: {e}")
        except Exception as e:
            logging.error(f"Failed to connect to PJM: {e}")

class Transformer:
    def __init__(self):
        pass

    async def pjm_da_hrl_lmps(self, data):
        value = data[0]['total_lmp_da']
        return value

class Loader(ETL):
    def __init__(self):
        super().__init__()

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
                    try:    
                        await session.post(
                            r["Links"]["Value"],
                            auth=aiohttp.BasicAuth(self.pi_user, self.pi_password),
                            ssl=False,
                            json=payload,
                            headers=self.pi_headers,
                        )
                    except Exception as e:
                        logging.error(f"Failed to load Pi value: {e}")
        except Exception as e:
            logging.error(f"Failed to connect to Pi database: {e}")

    async def pg(self, schema, table, columns, data):
        try:
            async with await asyncpg.connect (
                host = self.pg_host,
                database = self.pg_database,
                user = self.pg_user,
                password = self.pg_password,
            ) as pg:
                fields = ', '.join(columns)
                placeholders = ', '.join(['$' + str(i+1) for i in range(len(columns))])
                pg_insert = f"INSERT INTO {schema}.{table} ({fields}) VALUES ({placeholders})"
                try:
                    await pg.execute(pg_insert, *data)
                except (Exception, asyncpg.PostgresError) as e:
                    logging.error(f"Failed to execute Postgres query: {e}")
        except (Exception, asyncpg.PostgresError) as e:
            logging.error(f"Failed to connect to Postgres database: {e}")
