import psycopg2 as pg


class DbHelper:
    def __init__(
        self,
        config=None,
        user="admin",
        password="quest",
        host="questdb",
        port_pgsql_wire=8812,
        port_influxdb_line=9009,
        database="qdb",
    ):
        if config:
            if "user" in config.keys():
                user = config["user"]
            if "password" in config.keys():
                password = config["password"]
            if "host" in config.keys():
                host = config["host"]
            if "port_sql" in config.keys():
                port_pgsql_wire = config["port_sql"]
            if "port_influx" in config.keys():
                port_influxdb_line = config["port_influx"]
            if "database" in config.keys():
                database = config["database"]

        self.user = user
        self.password = password
        self.host = host
        self.port_pgsql = port_pgsql_wire
        self.port_influxdb = port_influxdb_line
        self.database = database

    def get_config(self) -> dict:
        config = {
            "user": self.user,
            "password": self.password,
            "host": self.host,
            "port_sql": self.port_pgsql,
            "port_influx": self.port_influxdb,
            "database": self.database,
        }
        return config

    def make_connection(self):
        connection = pg.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port_pgsql,
            database=self.database,
            # The following two options are needed when connecting from windows
            gssencmode="disable",
            sslmode="disable",
        )
        return connection

    def run_query(self, query: str, select: bool = False, verbose: bool = False):
        """
        Runs the given SQL query on the database
        """
        if verbose:
            print(f"Running query: {query}")

        connection = None
        cursor = None
        try:
            connection = self.make_connection()
            cursor = connection.cursor()
            cursor.execute(query)
            if select:
                return cursor.fetchall()

        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
            if verbose:
                print("Postgres connection is closed.")

    def set_user(self, user: str):
        self.user = user

    def set_password(self, password: str):
        self.password = password
    
    def set_host(self, host: str):
        self.host = host
    
    def set_pgsql_port(self, port: int):
        self.port_pgsql = port

    def set_influxdb_port(self, port: int):
        self.port_influxdb = port

    def set_database(self, database: str):
        self.database = database

    def get_user(self):
        return self.user

    def get_password(self):
        return self.password
    
    def get_host(self):
        return self.host
    
    def get_pgsql_port(self):
        return self.port_pgsql

    def get_influxdb_port(self):
        return self.port_influxdb

    def get_database(self):
        return self.database
