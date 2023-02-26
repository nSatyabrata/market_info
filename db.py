import asyncpg
from datetime import date
from typing import Optional, Dict, List


class DatabaseOperationError(Exception):
    """Custom error that is raised if there is any issue while operating with database."""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)



async def get_database_connection(config: Dict) -> Optional[asyncpg.connection.Connection]:
    try:
        conn = await asyncpg.connect(**config)
        return conn
    except Exception:
        return None


async def create_table(conn: asyncpg.connection.Connection) -> None:
    try:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS economy_data (
                ticker_name varchar(40) not null,
                dates date not null,
                values float,
                date_created date not null default current_date,
                primary key (ticker_name, dates, date_created)
            )
        ''')
    except Exception as error:
        raise DatabaseOperationError(f"Couldn't insert data due to {error=}.")

 
async def insert_data(data: List, conn: asyncpg.connection.Connection) -> None:
    
    if data is None or len(data) == 0:
        raise DatabaseOperationError(f"Data is not available.")
    else:
        try:
            await conn.copy_records_to_table('economy_data',
                                                records=data,
                                                columns=['ticker_name', 'dates', 'values'])
            
        except asyncpg.exceptions.UniqueViolationError:
            raise DatabaseOperationError(f"Data already exist.")
        except Exception as error:
            raise DatabaseOperationError(f"Couldn't insert data due to {error=}.")    



async def delete_old_data(conn: asyncpg.connection.Connection):

    try:
        result = await conn.fetch('''
                    SELECT MAX(date_created) FROM economy_data
                ''')
        
        if result[0]['max'] == date.today():
            await conn.execute('''
                DELETE FROM economy_data where date_created <> $1
            ''', result[0]['max'])
        else:
            raise DatabaseOperationError("New data is not available")
    except Exception as error:
        raise DatabaseOperationError(f"Could not delete data due to {error=}")


