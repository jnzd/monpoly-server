import psycopg2 as pg
from time import time

USER     = "admin"
PASSWORD = "quest"
HOST     = "localhost" # questdb
# HOST     = "172.28.128.1"
DATABASE = "qdb"

class DbHelper:
    def __init__(
        self,
        config=None,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port_pgsql_wire=8812,
        port_influxdb_line=9009,
        database=DATABASE,
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

    def run_query(self, query: str, select: bool = False,) -> dict:
        """
        Runs the given SQL query on the database
        """
        connection = None
        cursor = None
        try:
            connection = self.make_connection()
            cursor = connection.cursor()
            # t = time()
            cursor.execute(query)
            # print("query run time: ", time() - t)
            if select:
                return {"response": cursor.fetchall()}
            else:
                return {"response": "successfully executed query: " + query}
        except pg.OperationalError as error:
            return{"error": f"pg.OperationalError: {str(error)}"}
        except pg.DatabaseError as error:
            return{"error": f"pg.DatabaseError: {str(error)}"}
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()


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
