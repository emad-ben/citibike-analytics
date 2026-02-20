from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from .logger import setup_logger, log_function_call
import os

load_dotenv()
logger = setup_logger(__name__)

class DatabaseConnectionError(Exception):
    pass

class SQLFileExecutionError(Exception):
    pass

@log_function_call(logger)
def get_engine(echo=False):
    """
    create and return sqlalchemy database engine

    -- arguments --
        echo: log all generated sql statements (useful for debugging)

    -- return --
        sqlalchemy object as main interface to our postgres database
    """

    try:
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        db = os.getenv("DB_NAME")

        if not all([user, db]):
            raise ValueError("missing database credentials")
    
        db_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
        engine = create_engine(db_url, echo=echo)
    
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    
        return engine

    except ValueError as e:
        raise DatabaseConnectionError(str(e))
    
    except SQLAlchemyError as e:
        raise DatabaseConnectionError(str(e))

@log_function_call(logger)
def execute_sql_file(engine, sql_file_path):
    """
    execute a specific sql file

    -- arguments --
        engine: sqlalchemy engine object that serves as the interface to the postgres database
        sql_file_path: path to sql file to execute relative to project root
    """

    try:
        with open(sql_file_path, "r") as file:
            sql = file.read()
        
        with engine.connect() as connection:
            transaction = connection.begin()
            try:
                connection.execute(text(sql))
                transaction.commit()
            except Exception as e:
                transaction.rollback()
                raise SQLFileExecutionError(str(e))

    except Exception as e:
        raise SQLFileExecutionError(str(e))

def get_row_count(engine, schema_name, table_name):
    """
    get row count for a specific table within database

    -- arguments --
        engine: sqlalchemy engine object acts as interface to the database
        schema_name: schema name within database
        table_name: table name within schema
    -- return --
        row count as an integer
    """

    query = f"""
        SELECT COUNT(*) from {schema_name}.{table_name};
    """

    try:
        with engine.connect() as connection:
            result = connection.execute(text(query))
            row_count = result.scalar()
        return row_count
    except Exception as e:
        raise SQLAlchemyError(f"failure retrieving row count from {schema_name}.{table_name}")
