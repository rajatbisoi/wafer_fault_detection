class CheckExsistence_db:
    def __init__(self,DB_NAME,client):
        self.DB_NAME=DB_NAME
        self.client=client

    def checkExistence_DB(self):
        """It verifies the existence of DB"""
        DBlist = self.client.list_database_names()
        if self.DB_NAME in DBlist:
            # print(f"DB: '{self.DB_NAME}' exists")
            return True
        # print(f"DB: '{self.DB_NAME}' not yet present OR no collection is present in the DB")
        return False