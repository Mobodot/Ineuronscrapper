import pymongo
# from logs import Logger


class Mydb:

    def __init__(self, conn_string, log_obj):
        try:
            self.client = pymongo.MongoClient(conn_string)
            self.db_logs = log_obj
            self.db_logs.info("Connection established!")
        except ConnectionError as e:
            self.db_logs.error(f"Unable to establish connection! >>> {e}")

    def _check_existence_db(self, db_name):
        """
        Check if the database name exists or not
        :param DB_name: string
        :return: Boolean
        """
        db_list = self.client.list_database_names()
        if db_name in db_list:
            self.db = self.client[db_name]
            return True
        else:
            return False

    def create_database(self, db_name):
        """
        Creates a new database
        :param db_name: string
        :return:
        """
        try:
            if not self._check_existence_db(db_name):
                self.db = self.client[db_name]
                self.db_logs.info(f"{db_name} Database has been Created!")
        except Exception as e:
            self.db_logs.error(f"Unable to create {db_name} Database! >>> {e}")

    def _check_existence_coll(self, db_name, coll_name):
        """
        Checks if a collection name already exists
        :param COLL_name: string
        :return: Boolean
        """
        self._check_existence_db(db_name)
        coll_list = self.db.list_collection_names()
        # print(coll_list)
        if coll_name in coll_list:
            self.collection = self.db[coll_name]
            # print(self.collection)
            return True
        else:
            return False

    def create_collection(self, db_name, coll_name):
        """
        Creates a new collection
        :param db_name: string
        :param coll_name: string
        :return:
        """
        try:
            if not self._check_existence_coll(db_name, coll_name):
                self.collection = self.db[coll_name]
                self.db_logs.info(f"{coll_name} Collection has been Created!")
        except Exception as e:
            self.db_logs.error(f"Unable to create collection! >>> {e}")

    def bulk_insert(self, records):
        """
        Inserts bulk records into a collection
        :param db_name: string
        :param coll_name: string
        :param records: string
        :return:
        """
        try:
            self.collection.insert_many(records)
            self.db_logs.info(f"{len(records)} records inserted!")
        except Exception as e:
            self.db_logs.error(f"Unable to insert records! >>> {e}")

    def single_insertion(self, record):
        """
        Inserts a single record into a collection
        :param record: string
        :return:
        """
        try:
            self.collection.insert_one(record)
            self.db_logs.info(f"1+ records has been inserted into {self.collection.name}")
        except Exception as e:
            self.db_logs.error(f"Unable to insert record >>> {e}")

    def find_all_records(self, db_name, coll_name):
        """
        Retrieves all the records from a specific collection
        :param db_name: string
        :param coll_name: string
        :return: str or list
        """
        if self._check_existence_db(db_name) and self._check_existence_coll(db_name, coll_name):
            all_records = list(self.collection.find())
            self.db_logs.info(f"{len(all_records)} records found!")
            return all_records
        elif self._check_existence_db(db_name) and not self._check_existence_coll(db_name, coll_name):
            return "Collection does not exist!"
        else:
            return "Database does not exist!"

    def find_one_record(self, db_name, coll_name, course_name):
        """
        Returns a specific record if it exists in a collection
        :param db_name: str
        :param coll_name: str
        :param course_name: str
        :return: str or list
        """
        # collection = self.db.get_collection(self.collection.name)
        if self._check_existence_db(db_name) and self._check_existence_coll(db_name, coll_name):
            if course_name:
                course_detail_record = list(self.collection.find({"title": course_name.replace("-", " ")}))
                self.db_logs.info(f"{len(course_detail_record)} records found!")
                return course_detail_record
            else:
                course_category_record = list(self.collection.find({str(course_name): {"$exists": True}}))
                self.db_logs.info(f"{len(course_category_record)} records found!")
                return course_category_record
        elif self._check_existence_db(db_name) and not self._check_existence_coll(db_name, coll_name):
            return "Collection does not exist!"
        else:
            return "Database does not exist!"
