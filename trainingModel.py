"""
This is the Entry point for Training the Machine Learning Model.

Written By: iNeuron Intelligence
Version: 1.0
Revisions: None

"""


# Doing the necessary imports
from sklearn.model_selection import train_test_split
from data_ingestion import data_loader
import pandas as pd
import numpy as np
from data_preprocessing import preprocessing
from data_preprocessing import clustering
from best_model_finder import tuner
from file_operations import file_methods
from application_logging import logger
from to_mongo.to_db import To_mongo_db
from awss3_updown.aws_s3_operations import Aws_Bucket_operation
import pickle

#Creating the common Logging object


class trainModel:

    def __init__(self):
        self.log_writer = logger.App_Logger()
        self.mongo = To_mongo_db('wafer')
        self.aws = Aws_Bucket_operation()


        # self.file_object = open("Training_Logs/ModelTrainingLog.txt", 'a+')

    def trainingModel(self):
        # Logging the start of Training
        self.log_writer.log('wafer_log', 'Start of Training')
        try:
            # Getting the data from the source
            # data_getter=data_loader.Data_Getter(self.file_object,self.log_writer)
            data = self.mongo.downlaod_all_from_mongo('wafer_good_data','temp_db')


            """doing the data preprocessing"""

            preprocessor=preprocessing.Preprocessor('wafer_log', self.log_writer)
            data=preprocessor.remove_columns(data,['Wafer']) # remove the wafer column as it doesn't contribute to prediction.

            # create separate features and labels
            X,Y=preprocessor.separate_label_feature(data,label_column_name='Good/Bad')

            # check if missing values are present in the dataset


            # if missing values are there, replace them appropriately.
            X.replace(to_replace='NULL', value=np.nan, inplace=True) # consumes  4 sec to compute
            is_null_present = preprocessor.is_null_present(X)
            if(is_null_present):
                X=preprocessor.impute_missing_values(X)  # missing value imputation

            # check further which columns do not contribute to predictions
            # if the standard deviation for a column is zero, it means that the column has constant values
            # and they are giving the same output both for good and bad sensors
            # prepare the list of such columns to drop
            cols_to_drop=preprocessor.get_columns_with_zero_std_deviation(X) # consumes a lot of time
            # drop the columns obtained above
            X=preprocessor.remove_columns(X,cols_to_drop)
            """ Applying the clustering approach"""

            kmeans=clustering.KMeansClustering('wafer_log', self.log_writer) # object initialization.
            number_of_clusters=kmeans.elbow_plot(X)  #  using the elbow plot to find the number of optimum clusters

            # Divide the data into clusters
            X=kmeans.create_clusters(X,number_of_clusters)

            #create a new column in the dataset consisting of the corresponding cluster assignments.

            # X=pd.DataFrame.join(X,Y)
            X['Labels']=Y.values


            # getting the unique clusters from our dataset
            list_of_clusters=X['Cluster'].unique()

            """parsing all the clusters and looking for the best ML algorithm to fit on individual cluster"""

            for index,i in enumerate(list_of_clusters):
                cluster_data=X[X['Cluster']==i] # filter the data for one cluster
                # Prepare the feature and Label columns
                cluster_features=cluster_data.drop(['Labels','Cluster'],axis=1)
                cluster_label= cluster_data['Labels']

                # splitting the data into training and test set for each cluster one by one
                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label, test_size=1 / 3, random_state=355)

                model_finder=tuner.Model_Finder('wafer_log',self.log_writer) # object initialization

                #getting the best model for each of the clusters
                best_model_name,best_model=model_finder.get_best_model(x_train,y_train,x_test,y_test)

                #saving the best model to the directory.
                # file_op = file_methods.File_Operation('wafer_log',self.log_writer)
                # save_model=file_op.save_model(best_model,best_model_name+str(i))
                print(best_model)
                best_model = pickle.dumps(best_model)
                self.aws.Upload_To_S3_obj(best_model,best_model_name+str(index)+'.sav',bucket_prefix='wafer-model')

            # logging the successful Training
            self.log_writer.log('wafer_log', 'Successful End of Training')
            # self.file_object.close()

        except Exception as err:
            # logging the unsuccessful Training
            self.log_writer.log('wafer_log', 'Unsuccessful End of Training')
            # self.file_object.close()
            print(str(err))
            raise err