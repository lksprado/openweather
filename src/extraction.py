import requests
from datetime import datetime, date
import json 
from pathlib import Path
import logging 


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# IDENTIFY MISSING DAYS AND APPEND TO LIST
def read_missing(control_file: Path)-> list:
    control_file = Path(control_file)
    days = []
    with open(control_file, "r") as file:
        for line in file:
            days.append(line.split(",")[0].strip())
    return days

def get_day_summary(output_path: Path, dates_list:list, token:str ):
    output_path = Path(output_path)
    
    
    lat=-23.137
    lon=-46.5547861
    for d in dates_list:
        if isinstance(d, (datetime, date)):
            d=d.strftime("%Y-%m-%d")
        elif isinstance(d, str):
            d = datetime.strptime(d.strip(), "%Y-%m-%d").strftime("%Y-%m-%d")

        
        url = f'https://api.openweathermap.org/data/3.0/onecall/day_summary?lat={lat}&lon={lon}&date={d}&appid={token}'
        logger.info(f"GET: {url}")
        file_path = output_path / f'day_summary_{d}.json'
        response = requests.get(url)
        logger.info(response.status_code)
        if response.status_code == 200:
            with open (file_path, 'w') as f:
                json.dump(response.json(), f, indent=4)
            logger.info(f"Saved json --- {file_path}")

if __name__ == '__main__':
    from dotenv import load_dotenv
    import os
    from missing_raw import identify_missing_dates,write_list_to_csv
    load_dotenv()
    import psycopg2
    
    db_con = psycopg2.connect(host='localhost',
                        port=5435,
                        dbname="postgres",
                        user="postgres",
                        password="pg12345"
                        )
    
    try:
        ds_list = identify_missing_dates(db=db_con)
    finally:
        db_con.close()
    
    output = "."
    token = os.getenv("MY_API")
    get_day_summary(output_path=output,dates_list=ds_list,token=token)