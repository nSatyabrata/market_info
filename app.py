from config import db_config
from indicators import indicators
from db import get_database_connection, insert_data, delete_old_data, create_table, DatabaseOperationError
from utils import upload_logs_to_s3
from logger import get_logger, log_buffer
from datetime import datetime
import aiohttp, asyncio, asyncpg, json
from typing import Coroutine, List, Dict

logger = get_logger(name="market-info", buffer=log_buffer)


async def get_response_data(session: aiohttp.ClientSession, indicator: Dict) -> Coroutine:
    """
    Get json response from given URL.
    """
    try:
        async with session.get(indicator['url']) as response:
            return await response.json(content_type=None)
    except Exception as error:
        logger.error(f"Error while fetching data for \"{indicator['name']}\" due to {error=}.")
        raise ConnectionError()

   
async def get_all_data(indicators: Dict) -> List:
    """
    Get all the data from all available urls asynchronously.
    """
    async with aiohttp.ClientSession() as session:
        all_records = []
        try:
            responses = await asyncio.gather(*[get_response_data(session, indicator) for indicator in indicators], return_exceptions=True)
            error_count = 0
            for response in responses:
                if isinstance(response, ConnectionError) == False:
                    records = [( response['ticker'], datetime.strptime(record[0],'%Y-%m-%d').date() , record[1]) for record in zip(response['data']['dates'], response['data']['values'])]

                    logger.info(f"Found {len(records)} records for {response['description']}")
                    all_records += records
                else:
                    error_count += 1
            if error_count != 0:
                return []
        except Exception as error:
            logger.error(f"Error while getting data due to {error=}.")
            return []
      
        logger.info(f"Total {len(all_records)} records found.")
        return all_records
    
async def tasks(indicators: Dict, db_config: Dict):

    task1 = asyncio.create_task(get_database_connection(db_config))
    task2 = asyncio.create_task(get_all_data(indicators))  
    conn: asyncpg.connection.Connection = await task1
    data: list = await task2
    if conn:
        try:
            await create_table(conn)

            logger.info("Inserting new data.")
            await insert_data(data, conn)
            logger.info("Inserted data successfully.")

            logger.info("Started deletion process for old data.")
            await delete_old_data(conn)
            logger.info("Successfully deleted old data.")

        except Exception as error:
            raise DatabaseOperationError(f"Error occured with database. {error=}")
        
        finally:
            await conn.close()
    else:
        raise ConnectionError("Database connection error. Couldn't update the data.")
    


def main(event, context):
    
    message = ""
    try:
        asyncio.run(tasks(indicators, db_config))
        message = 'Success'
    except Exception as error:
        logger.error(error)
        message = 'Failed'
    
    upload_logs_to_s3(log_buffer)    
    log_buffer.close()

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps({
            "message":message
        })
    }
