import pymongo as pmg
from application_logging.logger import App_Logger
import pandas as pd
from bson import ObjectId
import json


class To_mongo_db:
    def __init__(self,sql_db_name='wafer'):
        self.logger = App_Logger(sql_db_name)

    def send_to_mongo(self,collection, DB_NAME, data, db_url='localhost:27017',delete=False,initial_columnname="Unnamed: 0"):
        """

                        Writen by; Rajat Bisoi
                        updates:NONE

                        input: it takes input database name, collection name,data in dataframe format and
                                database url as optional.
                        return: None

                        description: it sends files to mongodb server

        """
        DEFAULT_CONNECTION_URL = db_url  # 'mongodb+srv://wafer:wafer@waferdata.rdww5.mongodb.net/<dbname>?retryWrites=true&w=majority'
        # DB_NAME = "Training_Database"
        client = pmg.MongoClient(DEFAULT_CONNECTION_URL)

        conn = client[DB_NAME][collection]
        self.logger.log('wafer_log', 'connection to mongodb established!!')
        if delete==True:
            conn.drop()
        data.index = data[initial_columnname]
        # for row in data[initial_columnname]:
        #     d[row] = dict(data[data.columns.drop(initial_columnname)].loc[row])
        data = data.to_json()
        data = json.loads(data)
        conn.insert_one(data)
        self.logger.log('wafer_log', f'file uploaded to collection {collection} in db {DB_NAME}!')

    def send_to_mongo_raw(self,collection, DB_NAME, data, db_url='localhost:27017'):
        DEFAULT_CONNECTION_URL = db_url  # 'mongodb+srv://wafer:wafer@waferdata.rdww5.mongodb.net/<dbname>?retryWrites=true&w=majority'
        # DB_NAME = "Training_Database"
        client = pmg.MongoClient(DEFAULT_CONNECTION_URL)
        conn = client[DB_NAME][collection]
        conn.insert_one(data)

    def downlaod_from_mongo_raw(self, collection, DB_NAME, ID_name, db_url='localhost:27017'):
        DEFAULT_CONNECTION_URL = db_url  # 'mongodb+srv://wafer:wafer@waferdata.rdww5.mongodb.net/<dbname>?retryWrites=true&w=majority'
        # DB_NAME = "Training_Database"
        client = pmg.MongoClient(DEFAULT_CONNECTION_URL)
        conn = client[DB_NAME][collection]
        record = conn.find_one(ObjectId(ID_name['_id']))#ObjectId(str(ID_name['_id'])))
        return record


    def downlaod_all_from_mongo(self, collection, DB_NAME, db_url='localhost:27017'):
        DEFAULT_CONNECTION_URL = db_url  # 'mongodb+srv://wafer:wafer@waferdata.rdww5.mongodb.net/<dbname>?retryWrites=true&w=majority'
        # DB_NAME = "Training_Database"
        client = pmg.MongoClient(DEFAULT_CONNECTION_URL)

        conn = client[DB_NAME][collection]
        all_record = conn.find()
        lst=[]
        for idx, record in enumerate(all_record):
           lst.append(pd.DataFrame(record))
        df = pd.concat(lst)
        self.logger.log('wafer_log', f'collection {collection} downloaded from db {DB_NAME}!')
        return df.drop('_id',axis=1)

    def Repair_Downlaod_file(self,dic,initial_columnname="Unnamed: 0"):
        da1 = pd.DataFrame(dic).drop("_id", axis=1)
        return da1.rename(columns={'Unnamed: 0': initial_columnname})

    def Delete_collection(self,DB_NAME,collection,db_url='localhost:27017'):
        DEFAULT_CONNECTION_URL = db_url
        client = pmg.MongoClient(DEFAULT_CONNECTION_URL)
        conn = client[DB_NAME]
        lnames = conn.collection_names()
        if collection in lnames:
            conn[collection].drop()
        self.logger.log('wafer_log', f'collection {collection} droped from db {DB_NAME}!')

    def downlaod_one_from_mongo(self, collection, DB_NAME, ID_name, db_url='localhost:27017',initial_columnname="Unnamed: 0"):
        DEFAULT_CONNECTION_URL = db_url  # 'mongodb+srv://wafer:wafer@waferdata.rdww5.mongodb.net/<dbname>?retryWrites=true&w=majority'
        # DB_NAME = "Training_Database"
        client = pmg.MongoClient(DEFAULT_CONNECTION_URL)

        conn = client[DB_NAME][collection]
        record = conn.find_one(ObjectId(ID_name['_id']))

        return self.Repair_Downlaod_file(pd.DataFrame(record).reset_index().drop('index', axis=1), initial_columnname)

    def Get_ID(self, collection, DB_NAME, db_url='localhost:27017'):
        DEFAULT_CONNECTION_URL = db_url  # 'mongodb+srv://wafer:wafer@waferdata.rdww5.mongodb.net/<dbname>?retryWrites=true&w=majority'
        # DB_NAME = "Training_Database"
        client = pmg.MongoClient(DEFAULT_CONNECTION_URL)

        conn = client[DB_NAME][collection]
        id_obj=conn.find({}, {"_id"})
        idxt = []
        for idx in id_obj:
            idxt.append(idx)
        return idxt

    def Delete_obj_in_collection(self, collection, DB_NAME, ID_name, db_url='localhost:27017'):
        DEFAULT_CONNECTION_URL = db_url  # 'mongodb+srv://wafer:wafer@waferdata.rdww5.mongodb.net/<dbname>?retryWrites=true&w=majority'
        # DB_NAME = "Training_Database"
        client = pmg.MongoClient(DEFAULT_CONNECTION_URL)
        conn = client[DB_NAME][collection]
        conn.delete_one(ID_name)

    def Move_data_in_collections(self,from_collection,to_collection,DB_name,ID_name, db_url='localhost:27017'):
        try:
            data = self.downlaod_one_from_mongo(from_collection,DB_name,ID_name)
        except Exception as err:
            raise err
        try:
            self.send_to_mongo(to_collection,DB_name,data)
        except Exception as err:
            try:
                self.send_to_mongo_raw(to_collection,DB_name,data)
            except Exception as err1:
                raise [err, err1]
        try:
            self.Delete_obj_in_collection(from_collection,DB_name,ID_name)
        except Exception as err:
            raise err

