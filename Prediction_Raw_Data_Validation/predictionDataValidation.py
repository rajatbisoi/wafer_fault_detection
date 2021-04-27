import sqlite3
from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from application_logging.logger import App_Logger
from to_mongo.to_db import To_mongo_db
from awss3_updown.aws_s3_operations import Aws_Bucket_operation



class Prediction_Data_validation:
    """
               This class shall be used for handling all the validation done on the Raw Prediction Data!!.

               Written By: iNeuron Intelligence
               Version: 1.0
               Revisions: None

               """

    def __init__(self):
        # self.Batch_Directory = path
        self.schema_path = 'schema_prediction.json'
        self.logger = App_Logger()
        self.mongo = To_mongo_db('wafer')
        self.aws = Aws_Bucket_operation(local_file_name_address='config/bucket_name')


    def valuesFromSchema(self):
        """
                                Method Name: valuesFromSchema
                                Description: This method extracts all the relevant information from the pre-defined "Schema" file.
                                Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
                                On Failure: Raise ValueError,KeyError,Exception

                                 Written By: iNeuron Intelligence
                                Version: 1.0
                                Revisions: None

                                        """
        try:
            # with open(self.schema_path, 'r') as f:
            #     dic = json.load(f)
            #     f.close()

            id = self.mongo.Get_ID('schema_wafer_prediction', 'temp_db')
            dic = self.mongo.downlaod_from_mongo_raw('schema_wafer_prediction', 'temp_db', id[0])
            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']

            # file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            self.logger.log('wafer_log',message)

            # file.close()



        except ValueError:
            # 'wafer_log' = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log('wafer_log',"ValueError:Value not found inside schema_training.json")
            # 'wafer_log'.close()
            raise ValueError

        except KeyError:
            # 'wafer_log' = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log('wafer_log', "KeyError:Key value error incorrect key passed")
            # 'wafer_log'.close()
            raise KeyError

        except Exception as e:
            # 'wafer_log' = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log('wafer_log', str(e))
            # 'wafer_log'.close()
            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns


    def manualRegexCreation(self):

        """
                                      Method Name: manualRegexCreation
                                      Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                                                  This Regex is used to validate the filename of the prediction data.
                                      Output: Regex pattern
                                      On Failure: None

                                       Written By: iNeuron Intelligence
                                      Version: 1.0
                                      Revisions: None

                                              """
        regex = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    # Not used
    def createDirectoryForGoodBadRawData(self):

        """
                                        Method Name: createDirectoryForGoodBadRawData
                                        Description: This method creates directories to store the Good Data and Bad Data
                                                      after validating the prediction data.

                                        Output: None
                                        On Failure: OSError

                                         Written By: iNeuron Intelligence
                                        Version: 1.0
                                        Revisions: None

                                                """
        try:
            path = os.path.join("Prediction_Raw_Files_Validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Prediction_Raw_Files_Validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while creating Directory %s:" % ex)
            file.close()
            raise OSError

    # Not used
    def deleteExistingGoodDataTrainingFolder(self):
        """
                                            Method Name: deleteExistingGoodDataTrainingFolder
                                            Description: This method deletes the directory made to store the Good Data
                                                          after loading the data in the table. Once the good files are
                                                          loaded in the DB,deleting the directory ensures space optimization.
                                            Output: None
                                            On Failure: OSError

                                             Written By: iNeuron Intelligence
                                            Version: 1.0
                                            Revisions: None

                                                    """
        try:
            path = 'Prediction_Raw_Files_Validated/'
            # if os.path.isdir("ids/" + userName):
            # if os.path.isdir(path + 'Bad_Raw/'):
            #     shutil.rmtree(path + 'Bad_Raw/')
            if os.path.isdir(path + 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')
                file = open("Prediction_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"GoodRaw directory deleted successfully!!!")
                file.close()
        except OSError as s:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError
        # Not used
    def deleteExistingBadDataTrainingFolder(self):

        """
                                            Method Name: deleteExistingBadDataTrainingFolder
                                            Description: This method deletes the directory made to store the bad Data.
                                            Output: None
                                            On Failure: OSError

                                             Written By: iNeuron Intelligence
                                            Version: 1.0
                                            Revisions: None

                                                    """

        try:
            path = 'Prediction_Raw_Files_Validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                file = open("Prediction_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"BadRaw directory deleted before starting validation!!!")
                file.close()
        except OSError as s:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError
        #Not used
    def moveBadFilesToArchiveBad(self):


        """
                                            Method Name: moveBadFilesToArchiveBad
                                            Description: This method deletes the directory made  to store the Bad Data
                                                          after moving the data in an archive folder. We archive the bad
                                                          files to send them back to the client for invalid data issue.
                                            Output: None
                                            On Failure: OSError

                                             Written By: iNeuron Intelligence
                                            Version: 1.0
                                            Revisions: None

                                                    """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            path= "PredictionArchivedBadData"
            if not os.path.isdir(path):
                os.makedirs(path)
            source = 'Prediction_Raw_Files_Validated/Bad_Raw/'
            dest = 'PredictionArchivedBadData/BadData_' + str(date)+"_"+str(time)
            if not os.path.isdir(dest):
                os.makedirs(dest)
            files = os.listdir(source)
            for f in files:
                if f not in os.listdir(dest):
                    shutil.move(source + f, dest)
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Bad files moved to archive")
            path = 'Prediction_Raw_Files_Validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
            self.logger.log(file,"Bad Raw Data Folder Deleted successfully!!")
            file.close()
        except OSError as e:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise OSError




    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
            Method Name: validationFileNameRaw
            Description: This function validates the name of the prediction csv file as per given name in the schema!
                         Regex pattern is used to do the validation.If name format do not match the file is moved
                         to Bad Raw Data folder else in Good raw data.
            Output: None
            On Failure: Exception

             Written By: iNeuron Intelligence
            Version: 1.0
            Revisions: None

        """
        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        # self.deleteExistingBadDataTrainingFolder()
        # self.deleteExistingGoodDataTrainingFolder()
        # self.createDirectoryForGoodBadRawData()
        self.mongo.Delete_collection('temp_db', 'wafer_bad_data_prediction')
        self.mongo.Delete_collection('temp_db', 'wafer_good_data_prediction')

        # onlyfiles = [f for f in listdir(self.Batch_Directory)]

        '''
        try:
            f = open("Prediction_Logs/nameValidationLog.txt", 'a+')
            for filename in onlyfiles:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            shutil.copy("Prediction_Batch_files/" + filename, "Prediction_Raw_Files_Validated/Good_Raw")
                            self.logger.log(f,"Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                        else:
                            shutil.copy("Prediction_Batch_files/" + filename, "Prediction_Raw_Files_Validated/Bad_Raw")
                            self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        shutil.copy("Prediction_Batch_files/" + filename, "Prediction_Raw_Files_Validated/Bad_Raw")
                        self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    shutil.copy("Prediction_Batch_files/" + filename, "Prediction_Raw_Files_Validated/Bad_Raw")
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

            f.close()
            
            '''

        bucket_inst = self.aws.Create_S3_Bucket_Instance(bucket_prefix='wafer-prediction')
        try:
            # f = open("Training_Logs/nameValidationLog.txt", 'a+')
            for obj in bucket_inst.objects.all():
                data = self.aws.Download_From_S3(obj.key)
                if (re.match(regex, obj.key)):
                    splitAtDot = re.split('.csv', obj.key)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            # shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Good_Raw")
                            self.mongo.send_to_mongo('wafer_good_data_prediction', 'temp_db', data)
                            self.logger.log('wafer_log', f'file {obj.key} uploaded to collection wafer_good_data')

                        else:
                            # shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                            data = data.to_json()
                            data = json.loads(data)
                            self.mongo.send_to_mongo_raw('wafer_bad_data_prediction', 'temp_db', data)
                            self.logger.log('wafer_log',
                                            f'invalid file name  {obj.key} uploaded to collection wafer_bad_data')
                    else:
                        # shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                        data = data.to_json()
                        data = json.loads(data)
                        self.mongo.send_to_mongo_raw('wafer_bad_data_prediction', 'temp_db', data)
                        self.logger.log('wafer_log',
                                        f'invalid file name  {obj.key} uploaded to collection wafer_bad_data')
                else:
                    # shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                    data = data.to_json()
                    data = json.loads(data)
                    self.mongo.send_to_mongo_raw('wafer_bad_data_prediction', 'temp_db', data)
                    self.logger.log('wafer_log', f'invalid file name  {obj.key} uploaded to collection wafer_bad_data')

        except Exception as e:
            # f = open("Prediction_Logs/nameValidationLog.txt", 'a+')
            self.logger.log('wafer_log', "Error occured while validating FileName %s" % e)
            # f.close()
            raise e




    def validateColumnLength(self,NumberofColumns):
        """
                    Method Name: validateColumnLength
                    Description: This function validates the number of columns in the csv files.
                                 It is should be same as given in the schema file.
                                 If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                                 If the column number matches, file is kept in Good Raw Data for processing.
                                The csv file is missing the first column name, this function changes the missing name to "Wafer".
                    Output: None
                    On Failure: Exception

                     Written By: iNeuron Intelligence
                    Version: 1.0
                    Revisions: None

             """
        '''
        try:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f,"Column Length Validation Started!!")
            for file in listdir('Prediction_Raw_Files_Validated/Good_Raw/'):
                csv = pd.read_csv("Prediction_Raw_Files_Validated/Good_Raw/" + file)
                if csv.shape[1] == NumberofColumns:
                    csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    csv.to_csv("Prediction_Raw_Files_Validated/Good_Raw/" + file, index=None, header=True)
                else:
                    shutil.move("Prediction_Raw_Files_Validated/Good_Raw/" + file, "Prediction_Raw_Files_Validated/Bad_Raw")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)

            self.logger.log(f, "Column Length Validation Completed!!")
        except OSError:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e

        f.close()
        '''
        try:
            # f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log('wafer_log',"Column Length Validation Started!!")
            idx = self.mongo.Get_ID('wafer_good_data_prediction','temp_db')
            for file in idx:
                # csv = pd.read_csv("Training_Raw_files_validated/Good_Raw/" + file)
                try:
                    testfile = self.mongo.downlaod_one_from_mongo('wafer_good_data_prediction','temp_db',file)
                except Exception as err:
                    try:
                        testfile = self.mongo.downlaod_from_mongo_raw('wafer_good_raw_prediction','temp_db',file)
                    except Exception as err1:
                        self.mongo.Move_data_in_collections('wafer_good_data', 'wafer_bad_data_prediction', 'temp_db', file)
                        self.logger.log('wafer_log', "Invalid Column Length for the file !! File moved to "
                                                     "wafer_Bad_Raw_prediction collection ")
                        raise [err, err1]
                testfile = pd.DataFrame(testfile)
                if testfile.shape[1] == NumberofColumns:
                    pass
                else:
                    # shutil.move("Training_Raw_files_validated/Good_Raw/" + file, "Training_Raw_files_validated/Bad_Raw")
                    self.mongo.Move_data_in_collections('wafer_good_data_prediction', 'wafer_bad_data_prediction', 'temp_db', file)
                    self.logger.log('wafer_log', "Invalid Column Length for the file !! File moved to "
                                                 "wafer_Bad_Raw_prediction collection ")
            self.logger.log('wafer_log',"Column Length Validation Completed!!")
        except OSError:
            # f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log('wafer_log', f"Error Occured while moving the file {OSError}")
            # f.close()
            raise OSError
        except Exception as e:
            # f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log("wafer_log", f"Error Occured {e}")
            # f.close()
            raise e

    def deletePredictionFile(self):

        # if os.path.exists('Prediction_Output_File/Predictions.csv'):
        #     os.remove('Prediction_Output_File/Predictions.csv')
        self.mongo.Delete_collection('temp_db', 'prediction_output')

    def validateMissingValuesInWholeColumn(self):
        """
                                  Method Name: validateMissingValuesInWholeColumn
                                  Description: This function validates if any column in the csv file has all values missing.
                                               If all the values are missing, the file is not suitable for processing.
                                               SUch files are moved to bad raw data.
                                  Output: None
                                  On Failure: Exception

                                   Written By: iNeuron Intelligence
                                  Version: 1.0
                                  Revisions: None

                              """
        '''
        
        try:
            f = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Missing Values Validation Started!!")

            for file in listdir('Prediction_Raw_Files_Validated/Good_Raw/'):
                csv = pd.read_csv("Prediction_Raw_Files_Validated/Good_Raw/" + file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count+=1
                        shutil.move("Prediction_Raw_Files_Validated/Good_Raw/" + file,
                                    "Prediction_Raw_Files_Validated/Bad_Raw")
                        self.logger.log(f,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break
                if count==0:
                    csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    csv.to_csv("Prediction_Raw_Files_Validated/Good_Raw/" + file, index=None, header=True)
        except OSError:
            f = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()
        '''
        try:
            # f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log('wafer_log',"Missing Values Validation Started!!")

            idx = self.mongo.Get_ID('wafer_good_data_prediction', 'temp_db')
            for file in idx:
                # csv = pd.read_csv("Training_Raw_files_validated/Good_Raw/" + file)
                testfile = self.mongo.downlaod_one_from_mongo('wafer_good_data_prediction','temp_db' ,file)
                testfile = pd.DataFrame(testfile)
                count = 0
                for columns in testfile:
                    if (len(testfile[columns]) - testfile[columns].count()) == len(testfile[columns]):
                        count+=1
                        # shutil.move("Training_Raw_files_validated/Good_Raw/" + file, "Training_Raw_files_validated/Bad_Raw")
                        self.mongo.Move_data_in_collections('wafer_good_data_prediction', 'wafer_bad_data_prediction', 'temp_db', file)
                        self.logger.log('wafer_log',f"Invalid Column Length for the file!! File moved to wafer_bad_data_prediction :: {file}")
                        break
                if count==0:
                    # testfile.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    # testfile.to_csv("Training_Raw_files_validated/Good_Raw/" + file, index=None, header=True)
                    self.mongo.send_to_mongo('wafer_good_data_prediction','temp_db',testfile)
        except OSError:
            # f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log('wafer_log', "Error Occured while moving the file :: %s" % OSError)
            # f.close()
            raise OSError
        except Exception as e:
            # f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log('wafer_log', "Error Occured:: %s" % e)
            # f.close()
            raise e













