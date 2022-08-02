import pandas as pd
from local_settings import config
from sqlalchemy import create_engine

engine = create_engine(
f"""postgresql+psycopg2://\
{config['user']}:{config['password']}@\
{config['db_host']}:{config['db_port']}/\
{config['db']}""")

data = pd.read_excel(config['example_path'])

data = data[['meeting_id', 'domain', 'importance_type', 'members_cnt', 'subject',
            'meeting_place', 'meeting_initiator', 'init_fio',
            'init_legacyexchangedn_flg', 'member', 'member_fio',
            'member_legacyexchangedn_flg', 'member_status',
            'member_responce_status', 'meeting_start_dttm', 'meeting_end_dttm',
            'version', 'tabnum', 'initiator_tabnum']]

data.to_sql(con=engine,
            schema='smartc',
            name='meetings',
            index=False,
            if_exists='replace', # once
            chunksize=20000)

