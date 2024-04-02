from etl import ETL

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
