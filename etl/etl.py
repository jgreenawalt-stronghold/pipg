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
