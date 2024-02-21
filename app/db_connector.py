import psycopg2, os

# Parâmetros postgres
POSTGRES_DB = "painel"#os.getenv("POSTGRES_DB")
POSTGRES_HOST = "localhost" #os.getenv("POSTGRES_HOST")
POSTGRES_USER = "postgres" #os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = "postgres" #os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = "40001" #os.getenv("POSTGRES_PORT")


def get_postgres_connection():
    return psycopg2.connect(database= POSTGRES_DB,
                    host= POSTGRES_HOST,
                    user= POSTGRES_USER,
                    password= POSTGRES_PASSWORD,
                    port= POSTGRES_PORT)