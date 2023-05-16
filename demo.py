import pandas as pd
from sqlalchemy import create_engine
connstr = "mssql+pymssql://sa:Th@nfS@@192.168.78.160:1433/TFFY_sc?charset=utf8"
engine = create_engine(connstr, echo=True, max_overflow=5)
data = pd.read_csv(r'C:\Users\Administrator\Downloads\Outage_Data_(MBCD)_1683880259637.csv')
data.to_sql('platts_refinery_data', con=engine, if_exists='append', index=False)