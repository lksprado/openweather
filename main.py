import pandas as pd 
import json
import os 
from datetime import datetime, timedelta

# max_date = datetime(2025, 3, 25).date()
# now = datetime.now().date()

# # Se a diferença for maior ou igual a 1 dia, gera a lista de datas
# if (now - max_date).days >= 1:
#     missing_dates = [(max_date + timedelta(days=i)).strftime("%Y-%m-%d")
#                      for i in range(1, (now - max_date).days )]
#     print("Datas faltantes:", missing_dates)
# else:
#     print("Nenhuma data faltante.")

h = datetime.today().strftime('%Y-%m-%d')
print(h)