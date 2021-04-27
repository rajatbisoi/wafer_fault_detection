from datetime import datetime
# from DataTypeValidation_Insertion_Training.DataTypeValidation import dBOperation
import mysql.connector as connection


class App_Logger:
    """
        Mandatory parameter: Database name
    """

    def __init__(self, Database_name='wafer', conn=None):
        self.database_name = Database_name
        self.conn = conn
        # self.now = datetime.now()
        # self.date = self.now.date()
        # self.current_time = self.now.strftime("%H:%M:%S")

    def log(self, table_name, log_message):

        """
                written by:Rajat Bisoi
                discription: logs messages to sql server
                update:None

                :param table_name: sql table name
                :param log_message: message to be send
                :return: None
        """


        # file_object.write(str(self.date) + "/" + str(self.current_time) + "\t\t" + log_message +"\n")
        log_message=log_message.replace("'","-")
        query = "CREATE TABLE IF NOT EXISTS {0} (log_date_time DATETIME,log_message LONGTEXT)".format(table_name)

        # conn=dBOperation.dataBaseConnection(self.Database_name)
        if self.conn is None:
            conn = connection.connect(host="127.0.0.1", user="user1", passwd="potatobasket777", use_pure=True)
        # check if the connection is established
        # print(mydb.is_connected())
        #     mydb.close()
        cursor = conn.cursor()  # create a cursor to execute queries
        cursor.execute("USE {0}".format(self.database_name))
        cursor.execute(query)
        conn.commit()
        # print("Table Created!!")
        try:
            query = "INSERT INTO {0} VALUES ('{1} {2}','{3}')".format(table_name, datetime.now().date(), datetime.now().strftime("%H:%M:%S"),
                                                                      log_message)
            cursor.execute(query)
            conn.commit()
            conn.close()
        except Exception as err:
            print(err, "error occurred!")
            usr_dsn = input("\n error may be due to table format and data insert mismatch,input decision "
                            "in (Y,N) format to delete table and retry.\n Attention all loger data will be lost of 'Y' is chosen \n decision:")
            if usr_dsn.upper() == "Y":
                cursor.execute("DROP TABLE {0}".format(table_name))
                query = "CREATE TABLE IF NOT EXISTS {0} (log_date_time DATETIME,log_message LONGTEXT)".format(table_name)
                cursor.execute(query)
                conn.commit()
                query = "INSERT INTO {0} VALUES ('{1} {2}','{3}')".format(table_name, datetime.now().date(), datetime.now().strftime("%H:%M:%S"), log_message)
                cursor.execute(query)
                conn.commit()
                conn.close()
            elif usr_dsn.upper() == "N":
                raise err
            else:
                exit()
