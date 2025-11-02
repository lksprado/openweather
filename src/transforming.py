import pandas as pd 
from pathlib import Path
import logging
import json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def parsing_daily_weather(staging_dir: Path)->str:
    logger.info("Iniciando parser...")
    staging_dir = Path(staging_dir)
    data = []
    for file in staging_dir.iterdir():
        if file.suffix == ".json":
            try:
                with open(file,'r') as f:
                    content = json.load(f)
                df = pd.json_normalize(content)
                df.columns = [x.replace('.', '_') for x in df.columns]
                df['temperature_min'] = (df['temperature_min'].astype(float) - 273.15).round(2)
                df['temperature_max'] = (df['temperature_max'].astype(float) - 273.15).round(2)
                df['temperature_afternoon'] = (df['temperature_afternoon'].astype(float) - 273.15).round(2)
                df['temperature_night'] = (df['temperature_night'].astype(float) - 273.15).round(2)
                df['temperature_evening'] = (df['temperature_evening'].astype(float) - 273.15).round(2)
                df['temperature_morning'] = (df['temperature_morning'].astype(float) - 273.15).round(2)
                df['cloud_cover_afternoon'] = df['cloud_cover_afternoon'].astype(int)
                df['humidity_afternoon'] = df['humidity_afternoon'].astype(int)
                df['precipitation_total'] = df['precipitation_total'].astype(int)
                df['wind_max_direction'] = df['wind_max_direction'].astype(int)
                df['pressure_afternoon'] = df['pressure_afternoon'].astype(int)
                df['lat'] = df['lat'].astype(float)
                df['lon'] = df['lon'].astype(float)
                df = df.drop(columns=['tz','units'])
                data.append(df)
            except Exception as e:
                logger.warning(f"Json vazio ou inválido {file} -- {e}")
    all_dfs = pd.concat(data, ignore_index=True)
    file_dest = staging_dir / "all_dfs.csv"
    all_dfs.to_csv(file_dest,index=False)
    logger.info(f"Dados consolidados em: {file_dest}")
    return str(file_dest.absolute())