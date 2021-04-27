from datetime import datetime
from os import listdir
import pandas
import numpy as np
from application_logging.logger import App_Logger
from to_mongo.to_db import To_mongo_db


class dataTransformPredict:
    """
                 This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.

                 Written By: Rajat Bisoi
                 Version: 1.0
                 Revisions: None

                 """

    def __init__(self):
        self.goodDataPath = "Prediction_Raw_Files_Validated/Good_Raw"
        self.logger = App_Logger()
        self.mongo = To_mongo_db()

    def replaceMissingWithNull(self):

        """
                                Method Name: replaceMissingWithNull
                                Description: This method replaces the missing values in columns with "NULL" to
                                             store in the table. We are using substring in the first column to
                                             keep only "Integer" data for ease up the loading.
                                             This column is anyways going to be removed during prediction.

                                 Written By: Rajat Bisoi
                                Version: 1.0
                                Revisions: None

                                        """

        """               
                  
        try:
             log_file = open("Prediction_Logs/dataTransformLog.txt", 'a+')
             onlyfiles = [f for f in listdir(self.goodDataPath)]
             for file in onlyfiles:
                  csv = pandas.read_csv(self.goodDataPath+"/" + file)
                  csv.fillna('NULL',inplace=True)
                  # #csv.update("'"+ csv['Wafer'] +"'")
                  # csv.update(csv['Wafer'].astype(str))
                  csv['Wafer'] = csv['Wafer'].str[6:]
                  csv.to_csv(self.goodDataPath+ "/" + file, index=None, header=True)
                  self.logger.log(log_file," %s: File Transformed successfully!!" % file)
             #log_file.write("Current Date :: %s" %date +'\t' + "Current time:: %s" % current_time + "\t \t" +  + "\n")

        except Exception as e:
             self.logger.log(log_file, "Data Transformation failed because:: %s" % e)
             #log_file.write("Current Date :: %s" %date +"\t" +"Current time:: %s" % current_time + "\t \t" + "Data Transformation failed because:: %s" % e + "\n")
             log_file.close()
             raise e
        log_file.close()
        """
        try:
            # onlyfiles = [f for f in listdir(self.goodDataPath)]
            idx = self.mongo.Get_ID('wafer_good_data_prediction', 'temp_db')
            for file in idx:
                # csv = pandas.read_csv(self.goodDataPath+"/" + file)
                csv = self.mongo.downlaod_one_from_mongo('wafer_good_data_prediction', 'temp_db', file,
                                                         initial_columnname='Wafer')
                csv.fillna(np.nan, inplace=True)
                # #csv.update("'"+ csv['Wafer'] +"'")
                # csv.update(csv['Wafer'].astype(str))
                csv['Wafer'] = csv['Wafer'].str[6:]
                # csv.to_csv(self.goodDataPath+ "/" + file, index=None, header=True)
                self.mongo.send_to_mongo('wafer_good_data_prediction', 'temp_db', csv, initial_columnname='Wafer')
                self.mongo.Delete_obj_in_collection('wafer_good_data_prediction', 'temp_db', file)
                self.logger.log('wafer_log', str(file).replace("'", "-") + "  File Transformed successfully!!")
            # log_file.write("Current Date :: %s" %date +"\t" + "Current time:: %s" % current_time + "\t \t" +  + "\n")
        except Exception as e:
            self.logger.log('wafer_log', "Data Transformation failed because :" + str(e).replace("'", "-"))
            raise e
