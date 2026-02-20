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

def execute_sql_file(engine, sql_file_path):
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
