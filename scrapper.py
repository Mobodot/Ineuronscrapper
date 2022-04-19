import requests
import pandas as pd
import json
import time
# from db import Mydb
# from logs import Logger


class Ineuronscrapper:
    def __init__(self, db_obj):
        self.course_categories_url = "https://api.ineuron.ai/v1/course/sitemeta?platformType=main"
        self.db_obj = db_obj

    def get_course_data(self):
        """
        :return: A dictionary of all course related information
        """
        try:
            response = requests.get(self.course_categories_url)
            # time.sleep(2)
            courses_data = response.text
            return json.loads(courses_data)
        except ConnectionError as c:
            return f"Connection Failed! >>> {c}"
        except Exception as e:
            return f" An error occured while trying to fetch data! >>> {e}"

    def course_categories(self, db_name, coll_name):
        """
        Creates a list of all course categories
        and inserts them into a database.

        NB: if all course categories already exists in db
        it retrieves records from the db directly.
        :param db_name: string
        :param coll_name: string
        :return: list
        """
        if self.db_obj._check_existence_db(db_name):
            if len(self.db_obj.find_all_records(db_name, coll_name)) != 0:
                return self.db_obj.find_all_records(db_name, coll_name)
        else:
            all_data = self.get_course_data()
            grouped_courses = pd.DataFrame(all_data["data"]["categories"], columns=["title", "parent"])

            # creating course categories
            category_id = {
                "603fa55d8c5e6f4b0ce22d50": "Data Science",
                "6041220403889537f8de7fcf": "Development",
                "606d266dc2a66a048826938b": "Cloud",
                "606d2681c2a66a048826943f": "Devops",
                "606d2738c2a66a048826938e": "Programming",
                "612497c04fe868a9eec15678": "Marketing",
                "612497c04fe868a9eec15792": "Teaching & Academics",
                "61fcf261c5aea106db4bbc37": "Kids",
                "6221cc92760e3e03b3f819fa": "Community Courses"
            }

            courses = []
            for key, value in category_id.items():
                groups = {}
                course = grouped_courses[grouped_courses["parent"] == key]
                groups[value] = list(course.to_dict()["title"].values())
                courses.append(groups)
            # self.db_obj.create_collection(coll_name)
            # self.course_cat_collection = coll_name
            self.db_obj.create_database(db_name)
            self.db_obj.create_collection(db_name, coll_name)
            self.db_obj.bulk_insert(courses)
            return courses

    def _extract_course_details(self, course_dict):
        """
        Utility function to filter needed data from all the data.
        :param course_dict: dict
        :return: dict
        """
        title = json.loads(course_dict)["pageProps"]["data"]["title"]
        description = json.loads(course_dict)["pageProps"]["data"]["details"]["description"]
        inr_price = json.loads(course_dict)["pageProps"]["data"]["details"]["pricing"]["IN"]
        dollar_price = json.loads(course_dict)["pageProps"]["data"]["details"]["pricing"]["US"]
        learn = json.loads(course_dict)["pageProps"]["data"]["meta"]["overview"]["learn"]
        requirements = json.loads(course_dict)["pageProps"]["data"]["meta"]["overview"]["requirements"]
        course_features = json.loads(course_dict)["pageProps"]["data"]["meta"]["overview"]["features"]

        curriculum_details = json.loads(course_dict)["pageProps"]["data"]["meta"]["curriculum"]

        curriculum_topics = []
        for key, value in curriculum_details.items():
            curriculum_topics.append(value["title"])

        course_details = {}
        course_details["title"] = title
        course_details["description"] = description
        course_details["inr_price"] = inr_price
        course_details["dollar_price"] = dollar_price
        course_details["learn"] = learn
        course_details["requirements"] = requirements
        course_details["course_features"] = course_features
        course_details["topics_covered"] = curriculum_topics
        return course_details


    def get_course_detail(self, db_name, coll_name, search_string):
        """
        Obtains course detail from ineuron site if course detail
        doesn't already exist in db.
        :param db_name: string
        :param coll_name: string
        :param search_string: string
        :return:
        """
        # api reference to each course
        # check if course_details already exists in db

        try:
            result = self.db_obj.find_one_record(db_name, coll_name, search_string)
            if len(result) != 0:
                return result

            else:
                # create a new collection for course details and insert course
                course_detail_url = "https://courses.ineuron.ai/_next/data/2B9IExsna5vRWhIwE3N6M/" + search_string + ".json"
                response = requests.get(course_detail_url)

                # if a page is found based on the search
                if response.status_code == 200:
                    record = self._extract_course_details(response.text)
                    self.db_obj.single_insertion(record)
                    # self.db_obj.course_detail_collection = coll_name
                    return record
                else:
                    return f"No records for {search_string}!"
        except ConnectionError as c:
            return f"Connection Failed! >>> {c}"
        except Exception as e:
            return f"An Error occurred while trying to obtain the record  >>> {e}"
