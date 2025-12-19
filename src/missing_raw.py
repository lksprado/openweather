from datetime import datetime, timedelta
import csv

import logging 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _get_first(db, sql: str):
    """Suporta PostgresHook (.get_first) ou psycopg2 (.cursor().fetchone())."""
    # PostgresHook do Airflow
    if hasattr(db, "get_first") and callable(db.get_first):
        return db.get_first(sql)
    # psycopg2 connection
    if hasattr(db, "cursor") and callable(db.cursor):
        with db.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()
    raise TypeError("db deve ser PostgresHook ou conexão psycopg2.")


def identify_missing_dates(db)-> list:    
    logger.info("Obtendo data maxima no DW")

    query_daily = f"SELECT MAX(date) :: DATE AS DT FROM raw.openweather_daily"
    
    result_daily  = _get_first(db, query_daily)
    
    max_date_daily = datetime.strptime(str(result_daily[0]), '%Y-%m-%d').date() 
    

    logger.info(f"Data inicial encontrada: {max_date_daily}")

    # Step 2: Determina até onde ir
    now = datetime.now()
    last_date = now.date() if now.hour >= 20 else (now - timedelta(days=1)).date()
    delta_days = (last_date - max_date_daily).days

    logger.info(f"Última data possível: {last_date} | Dias de diferença: {delta_days}")

    # Step 3: Gera e escreve CSV
    if delta_days <= 0:
        logger.info("Nenhuma data faltando. Limpando arquivo de controle.")
        return
    
    missing_dates = [
        (max_date_daily + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(1, delta_days+1) # +1 porque o range de 1 a 1 é zero/nulo, sempre e sempre deve haver pelo menos 1 de diferença
    ]
    
    if missing_dates:
        return missing_dates
        
    else:
        logger.info("Nenhuma data encontrada.")

def write_list_to_csv(input_ls: list, output_filepath):
    with open(output_filepath, mode='w') as file:
        writer = csv.writer(file)
        for missing_date in input_ls:
            writer.writerow([missing_date])
    logger.info(f"Arquivo salvo em: {output_filepath}")


if __name__ == '__main__':
    import psycopg2
    
    db_con = psycopg2.connect(host='localhost',
                        port=5435,
                        dbname="postgres",
                        user="postgres",
                        password="pg12345"
                        )
    
    try:
        identify_missing_dates(db=db_con)
    finally:
        db_con.close()

