import re
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import requests
import json
import pprint
import pandas as pd
import MySQLdb
from pymongo import MongoClient
import time
from datetime import datetime

# Load data from MySql
class StoresUpdate():

    def __init__(self, MySqlPasswd, google_key, mongoPasswd):
        self.passwd = MySqlPasswd
        self.google_key = google_key
        self.mongoPasswd = mongoPasswd
        self.place_ids = []
        # Connect to Local MySql
        self.StoreDB = MySQLdb.connect(host='127.0.0.1',
                                       user='root',
                                       passwd=self.passwd,
                                       db='google_stores',
                                       charset='utf8')
        self.cursor = self.StoreDB .cursor()
        # Connect to MongoDB
        self.client = MongoClient('mongodb+srv://' + 'HCHsu' + ':' + self.mongoPasswd +
                                  '@cluster0.j1l6l.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')


    def obtain_needed_placeid(self):
        """
        Get the needed place ids from MySql whicjh is sorted by user_ratings_total.
        From the dataframe, get the head 10 rows places each time.
        """
        # Get Original Data From MySql
        stores_sql = "SELECT place_id, user_ratings_total FROM store_info "
        stores = self.cursor.execute(stores_sql) # , values)
        stores = self.cursor.fetchall()
        field_names = [i[0] for i in self.cursor.description]

        # Get Already Updata Data From MySql
        stores_sql_U = "SELECT place_id FROM update_info "
        stores_U = self.cursor.execute(stores_sql_U) # , values)
        stores_U = self.cursor.fetchall()

        self.StoreDB.commit()
        self.cursor.close()

        # Convert tuple data to dataframe
        df = pd.DataFrame(stores, columns=field_names)
        df = df.sort_values(by='user_ratings_total', ascending=False).reset_index(drop=True)
        dfU = pd.DataFrame(stores_U, columns=['place_id'])

        # Filter out the same place_id in dfU
        df = df[~df['place_id'].isin(dfU['place_id'])]
        self.place_ids = df['place_id'].head(10).tolist()

        print('Obtain The Needed Placeid From MySql!')

        return self.place_ids

    def insert_data2mongo(self):
        """
        Retrieve the Json file of each place_ids and store into mongoDB.
        """
        # Connect to MongoDB
        db = self.client['google_stores']
        collection = db['storesinfo']

        # Load data to MongoDB
        for place_id in self.place_ids:
            # Sleep
            time.sleep(5)

            # Get Store Info From Google API
            url = 'https://maps.googleapis.com/maps/api/place/details/json?placeid=' + place_id +\
                  '&key=' + self.google_key
            response = requests.get(url)
            decoded_data = response.text.encode().decode('utf-8-sig')
            data = json.loads(decoded_data)
            data = data['result']

            # Insert Data into MongoDB
            collection.insert_one(data)

        print("Insert new data to MongoDB!")

    def copy_data2mysql(self):
        """
        Copy the new data set into MySql in New table.
        """
        # Connect to MongoDB
        db = self.client['google_stores']
        collection = db['storesinfo']

        # Collect Data From MongoDB
        dfF = pd.DataFrame()

        for place_id in self.place_ids:
            # Insert time to DF
            now = datetime.now()

            # MongoDb Data
            SInfo = collection.find_one({'place_id': place_id})
            Rlist = [x for x in SInfo.keys()]

            # Check every key exist or not
            cols = ['place_id', 'rating', 'user_ratings_total', 'url', 'website',
                    'formatted_phone_number', 'formatted_address']
            kk = [i for i in cols if i not in Rlist]
            for k in kk:
                SInfo[k] = 'nan'

            # Insert into df
            df = pd.DataFrame({'place_id': [place_id],
                               'rating': SInfo['rating'],
                               'user_ratings_total': SInfo['user_ratings_total'],
                               'url': SInfo['url'],
                               'website': SInfo['website'],
                               'formatted_phone_number': SInfo['formatted_phone_number'].replace(' ', '-'),
                               'formatted_address': SInfo['formatted_address'].replace(' ', ''),
                               'Update_time': [now.strftime("%Y-%m-%d")]
                               })
            dfF = pd.concat([dfF, df])

        for row in dfF.iterrows():
            list = row[1].values
            self.cursor.execute('INSERT INTO update_info(place_id, rating, user_ratings_total, url,\
                           website, formatted_phone_number, formatted_address, Update_time)'
                           'VALUES("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")'
                           % tuple(list))

        self.StoreDB.commit()
        self.cursor.close()

        print("Copy the New Data to MySql")


MySqlPasswd = 'ji3k27mysql'
google_key = ''
mongoPasswd = '$rW8QTpd6D5vS8m'

SU = StoresUpdate(MySqlPasswd, google_key, mongoPasswd)
SU.obtain_needed_placeid()


    
default_args = {
    'owner': 'HCHsu',
    'depends_on_past': False,
    'start_date': datetime(2021, 5, 27),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

places_update_dag = DAG(
    dag_id='places_update_dag',
    description='Update Info',
    default_args=default_args,
    schedule_interval='*/30 * * * *'
)

data2mongo = PythonOperator(
    task_id='Data_2_Mongo',
    execution_timeout=timedelta(seconds=120),
    python_callable=SU.insert_data2mongo,
    dag=places_update_dag)

mongo2mysql = PythonOperator(
    task_id='Mongo_2_MySql',
    execution_timeout=timedelta(seconds=120),
    python_callable=SU.copy_data2mysql,
    dag=places_update_dag)


data2mongo >> mongo2mysql
