"""
Загружаем данные из CBR
"""

import csv
import logging
from airflow import DAG
from datetime import datetime
import xml.etree.ElementTree as ET


from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'p_ganzhela',
    'poke_interval': 600,
    'csv_filepath' : '/tmp/last_cbr.xml'
}

with DAG("p_ganzhela_load_cbr",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['p_ganzhela']
         ) as dag:


    def xml_to_csv_func():
        parser = ET.XMLParser(encoding="UTF-8")
        tree = ET.parse(DEFAULT_ARGS['csv_filepath'], parser=parser)
        root = tree.getroot()

        with open(DEFAULT_ARGS['csv_filepath'], 'w') as csv_file:
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for Valute in root.findall('Valute'):
                NumCode = Valute.find('NumCode').text
                CharCode = Valute.find('CharCode').text
                Nominal = Valute.find('Nominal').text
                Name = Valute.find('Name').text
                Value = Valute.find('Value').text
                writer.writerow([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                                [Name] + [Value.replace(',', '.')])
                logging.info([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                             [Name] + [Value.replace(',', '.')])


    export_cbr_xml = BashOperator(
        task_id='export_cbr_xml',
        bash_command='rm -f {filepath} > /dev/null && curl {url} | iconv -f Windows-1251 -t UTF-8 > {filepath}'.format(
            url="""https://www.cbr.ru/scripts/XML_daily.asp?date_req={{execution_date.format('dd/mm/yyyy')}}""",
            filepath = DEFAULT_ARGS['csv_filepath']
        ), dag=dag
    )


    xml_to_csv = PythonOperator(
        task_id='xml_to_csv',
        python_callable=xml_to_csv_func
    )

    
    export_cbr_xml >> xml_to_csv
