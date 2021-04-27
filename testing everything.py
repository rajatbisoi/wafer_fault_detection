from to_mongo.to_db import To_mongo_db
from data_preprocessing import preprocessing
import pandas as pd
from sklearn.cluster import KMeans
from data_preprocessing import preprocessing
import numpy as np
import csv
from awss3_updown.aws_s3_operations import Aws_Bucket_operation
import joblib
from datetime import datetime
import re
import json
from application_logging.logger import App_Logger
from DataTransform_Training.DataTransformation import dataTransform
from application_logging import logger
from io import BytesIO

# bytesIO = BytesIO()
awsopp = Aws_Bucket_operation(local_file_name_address='config/bucket_name')
import pickle
from data_ingestion import data_loader
# b=open('schema_training.json')

# data = json.loads(b.read())


# awsopp.Create_Bucket()
# print(open('config/bucket_name').read())
# a='15'
# X = [[0.5, 1.], [-1., -1.5], [0., -2.]]
# y = [1, -1, -1]
# from sklearn.svm import LinearSVC
# linear_svc = LinearSVC()
# model = linear_svc.fit(X, y)
# model_bin = pickle.dumps(model)
# model1 = pickle.loads(model)
# # bytesIO.write(a)
# # bytesIO.seek(0)
# print(model_bin)
#
# awsopp.Create_Bucket('wafer-prediction')
# awsopp.Upload_Everything_To_S3(folder_in_local='Prediction_Batch_files',bucket_prefix='wafer-prediction')
# model = awsopp.Download_From_S3_raw('model-01.sav','wafer-model')
# model = pickle.loads(model)
# print(model.predict(X))
# awsopp.Delete_From_S3('model-01.sav','wafer-model')
# a=awsopp.Download_From_S3(r'training-raw-data-wafer/')


# c=a.Repair_Downlaod_file(b)
# awsopp.Upload_Everything_To_S3()
# print(c)
# temp = awsopp.Create_S3_Bucket_Instance()
# for obj in temp.objects.all():
#     a=awsopp.Download_From_S3(obj.key)
#     print(a)
# exit()
# b = a.Get_ID('iNeuron_Products', 'temp_db')
# idxt = []
# for idx in b:
#     idxt.append(idx)
# print(idxt[0])
# a.Move_data_in_collections('iNeuron_Products','wafer', 'temp_db',idxt[0])
# for c in idxt:
#     print(c)
#     print(a.downlaod_one_from_mongo('wafer', 'temp_db', c))
# print(data)
# a.send_to_mongo_raw('schema_wafer_training','temp_db',data)

# logger = App_Logger('wafer')
# e="Data Transformation failed because:: document must be an instance of dict, bson.son.SON, bson.raw_bson.RawBSONDocument, or a type 'that' inherits from collections.MutableMapping"
# logger.log('wafer_log', f"Data Transformation failed because :"+str(e).replace("'","-"))
# data = pd.read_csv('Training_Batch_Files/wafer_07012020_041011.csv',encoding='utf-8')
# a.send_to_mongo('wafer_good_data',"temp_db",data,)
# a=dataTransform()
# b=a.replaceMissingWithNull()
from bson import ObjectId


# id = mongo.Get_ID('schema_wafer_training', 'temp_db')
# # print(type(id[0]))
#
# dic = mongo.downlaod_from_mongo_raw('schema_wafer_training','temp_db', id[0])
# print(dic)
# import pymongo as pmg
# # mongo.Delete_collection('temp_db','wafer_bad_data')
# db_url='localhost:27017'
# DEFAULT_CONNECTION_URL = db_url
# client = pmg.MongoClient(DEFAULT_CONNECTION_URL)
# conn = client['temp_db']
# lnames = conn.list_collection_names()
# print(lnames)
# from Training_Raw_data_validation.rawValidation import Raw_Data_validation
# #
# regx = Raw_Data_validation()
# rex1=regx.manualRegexCreation()
# bucket_inst = awsopp.Create_S3_Bucket_Instance()
# for obj in bucket_inst.objects.all():
#     if (re.match(rex1, 'Wafer_25012020_142112')):
#         print('true', obj.key)
#     splitAtDot = re.split('.csv', 'Wafer_25012020_142112')
#     # print(splitAtDot)
#     splitAtDot = (re.split('_', splitAtDot[0]))
#     print(splitAtDot)
# now = datetime.now()
# date = now.date()
# current_time = now.strftime("%H:%M:%S")
#
# print(datetime.now().date(),datetime.now().strftime("%H:%M:%S"))
# bucket_name=None
# file_name='Wafer12_20012.csv'
# if bucket_name == None:
#     bucket_name = str(open('config/bucket_name').read())
# content = awsopp.s3_resource.Object(bucket_name, file_name)
# content.get()['Body'].read().decode('utf-8')
# data=pd.read_csv(content.get()['Body'])
# data.index = data['Unnamed: 0']
# d = {}
# for row in data['Unnamed: 0']:
#     d[row] = dict(data[data.columns.drop('Unnamed: 0')].loc[row])
# # self.logger.log('wafer_log', 'file {0} downloaded from s3!'.format(file_name))
# print(d)
# data=pd.read_csv('Training_Batch_Files/wafer_23012020_041211.csv')
# # mongo.send_to_mongo('test111','temp_db',data)
# a=mongo.downlaod_one_from_mongo('test111','temp_db',{'_id':"60369ee80903a21e78661404"},initial_columnname="Wafer")
# print(pd.DataFrame(a)

# log_writer = logger.App_Logger()
# file_object = open("Training_Logs/ModelTrainingLog.txt", 'a+')
# data_getter=data_loader.Data_Getter(file_object)
# data=data_getter.get_data()
# print(data)

# mongo = To_mongo_db('wafer')
# #
# b=mongo.downlaod_all_from_mongo(collection='wafer_good_data', DB_NAME='temp_db')
# # print(mongo.Repair_Downlaod_file(pd.DataFrame(b).T,initial_columnname='Wafer'))
# print(b.drop('_id',axis=1))
#
# log_writer = logger.App_Logger()
# preprocessor=preprocessing.Preprocessor('wafer_log',log_writer)
#
#
#
#
# data = mongo.downlaod_all_from_mongo('wafer_good_data','temp_db')
# # data=data.drop('_id',axis=1)
# print(data)
#
# # data = preprocessor.remove_columns(data, ['Wafer'])  # remove the wafer column as it doesn't contribute to prediction.
#
# # create separate features and labels
# x, Y = preprocessor.separate_label_feature(data, label_column_name='Good/Bad')
# x.replace(to_replace='NULL',value=np.nan,inplace=True)
# # check if missing values are present in the dataset
# is_null_present = preprocessor.is_null_present(x)
#
# # if missing values are there, replace them appropriately.
# if (is_null_present):
#     x = preprocessor.impute_missing_values(x)  # missing value imputation
#
#
#
# # check further which columns do not contribute to predictions
# # if the standard deviation for a column is zero, it means that the column has constant values
# # and they are giving the same output both for good and bad sensors
# # prepare the list of such columns to drop
# columns=x.columns
# col={a:'float64' for a in x.columns if x[a].dtype not in ['int64','float64']}
# x.astype(col,inplace=True)
# data_n = x.describe()
# print(data_n)
#
# print(x)
# cols_to_drop = preprocessor.get_columns_with_zero_std_deviation(x)
#
# # drop the columns obtained above
# x = preprocessor.remove_columns(x, cols_to_drop)
#
# # wcss=[]
# # print(x)
# # for i in range(1, 11):
# #     kmeans = KMeans(n_clusters=i, init='k-means++', random_state=42)  # initializing the KMeans object
# #     kmeans.fit(x)  # fitting the data to the KMeans Algorithm
# #     print(kmeans.inertia_)
# #     wcss.append(kmeans.inertia_)
#
#
#
# print(Y)
# x['Labels'] = Y.values
#
#
# print(x)
#
# list_of_clusters = x['Cluster'].unique()
# print('here')
# """parsing all the clusters and looking for the best ML algorithm to fit on individual cluster"""
#
# for i in list_of_clusters:
#     cluster_data = x[x['Cluster'] == i]  # filter the data for one cluster
#     print('here')  # reached here  now error is "['Labels'] not found in axis"
#     # Prepare the feature and Label columns
#     cluster_features = cluster_data.drop(['Labels', 'Cluster'], axis=1)
#     cluster_label = cluster_data['Labels']
#
#
# with open('file.txt', 'r') as searchfile:
#     for line in searchfile:
#         if 'hello' in line:
#             print(line)

# bucket_name=None
# if bucket_name == None:
#     with open('config/bucket_name', 'r') as name_store:
#         for line in name_store:
#             if 'wafer-training' in line:
#                 bucket_name = str(line)
#
# print(bucket_name)
# data=pd.read_csv('Prediction_Batch_files/wafer_22022020_041119.csv')
# mongo = To_mongo_db('wafer')
# mongo.send_to_mongo('wafer_good_data_prediction','temp_db',data)


# from file_operations import file_methods
#
# file_object = 'wafer_log'
# log_writer = logger.App_Logger()
# from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation
# from data_ingestion import data_loader_prediction
#
# pred_data_val = Prediction_Data_validation()
# data_getter=data_loader_prediction.Data_Getter_Pred(file_object,log_writer)
# data=data_getter.get_data()
# file_loader = file_methods.File_Operation(file_object, log_writer)
# kmeans = file_loader.load_model('kmeans.sav')
# print(kmeans)
# print(data)
# # mongo.Repair_Downlaod_file(data,initial_columnname='Wafer')
# clusters = kmeans.predict(data.drop(['Unnamed: 0'], axis=1))  # drops the first column for cluster prediction
# print(clusters)
# data['clusters'] = clusters
# print(clusters)
# clusters = data['clusters'].unique()
# print(clusters)


# for i in awsopp.Create_S3_Bucket_Instance(bucket_prefix="wafer-model").objects.all():
#     print(i.key)


def find_correct_model_file(cluster_number):
    """
                        Method Name: find_correct_model_file
                        Description: Select the correct model based on cluster number
                        Output: The Model file
                        On Failure: Raise Exception

                        Written By: iNeuron Intelligence
                        Version: 1.0
                        Revisions: None
            """
    # self.logger_object.log(file_object, 'Entered the find_correct_model_file method of the File_Operation class')
    try:
        cluster_number = cluster_number
        print('cluster no.:', cluster_number)
        # self.folder_name=self.model_directory
        list_of_model_files = []
        list_of_files = awsopp.Create_S3_Bucket_Instance(bucket_prefix="wafer-model").objects.all()
        for file in list_of_files:
            # print([i for i in file.key],cluster_number)
            try:
                if str(cluster_number) in list(file.key):
                        model_name = file
            except:
                continue
        # print(model_name)
        # model_name = model_name.split('.')[0]
        # logger_object.log(self.file_object,
        #                        'Exited the find_correct_model_file method of the Model_Finder class.')
        return model_name.key
    except Exception as e:
        # selogger_object.log(self.file_object,
        #                        'Exception occured in find_correct_model_file method of the Model_Finder class. Exception message:  ' + str(
        #                            e))
        # self.logger_object.log(self.file_object,
        #                        'Exited the find_correct_model_file method of the Model_Finder class with Failure')
        raise Exception()





if __name__=="__main__":
    mdl_nmae=find_correct_model_file(2)
    print(mdl_nmae)