import requests
import logging
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class GanzhelaLocationCountOperator(BaseOperator):
    ui_color = "#c19fc4"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_page_count(self, api_url: str) -> int:
        """
        Get count of page in API
        :param api_url
        :return: page count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise Exception('Error in load page count')

    def get_location_count_on_page(self, result_json: list) -> list:
        """
        Get count of locations in one page
        :param result_json:
        :return: locations_on_page
        """
        locations_on_page = []
        for one_location in result_json:
            location_id = one_location.get('id')
            location_name = one_location.get('name')
            location_type = one_location.get('type')
            location_dimension = one_location.get('dimension')
            location_residents_cnt = len(list(one_location.get('residents')))
            locations_on_page.append([location_id, location_name, location_type, location_dimension, location_residents_cnt])
        return locations_on_page



    def execute(self, context):
        """insert Rick&Morty location info to db"""
        location_residents_cnt = pd.DataFrame(columns=['id','name','type','dimension','residents_cnt'])
        ram_location_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(self.get_page_count(ram_location_url.format(pg='1'))):
            r = requests.get(ram_location_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                locations_from_page = self.get_location_count_on_page(r.json().get('results'))
                new_locations = pd.DataFrame(locations_from_page, columns=['id','name','type','dimension','residents_cnt'])
                location_residents_cnt = location_residents_cnt.append(new_locations)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise Exception('Error in load from Rick&Morty API')
        location_residents_cnt = location_residents_cnt.drop_duplicates(subset=['id'])
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        engine = pg_hook.get_sqlalchemy_engine()
        table_for_db = location_residents_cnt.reset_index(drop=True).sort_values('residents_cnt',ascending=False).head(3)
        table_for_db.to_sql('p_ganzhela_11_ram_location', con = engine, if_exists='replace', index = False)
        logging.info(f'INSERT {table_for_db.name.unique()} to DB')

