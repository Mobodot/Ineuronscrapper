from scrapper import Ineuronscrapper
from logs import Logger
from db import Mydb
from flask import Flask, request, jsonify

conn_string = "mongodb+srv://test:test@cluster0.e1r95.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
# DB_name = "IneuronScrapper"
# COLLECTION_name = "Ineuroncourses"

lg = Logger("db1")
test = Mydb(conn_string, lg)
scrap = Ineuronscrapper(test)


app = Flask(__name__)


@app.route("/course_categories", methods=["POST"])
def all_courses():
    if request.method == "POST":
        db_name = request.form["db_name"]
        coll_name = request.form["coll_name"]
        return jsonify({"course_categories": str(scrap.course_categories(db_name, coll_name))})


@app.route("/course_categories/course/details", methods=["POST"])
def specific_course():
    if request.method == "POST":
        db_name = request.form["db_name"]
        coll_name = request.form["coll_name"]
        course_name = request.form["course_name"]
        return jsonify({"course_details": str(scrap.get_course_detail(db_name, coll_name, course_name))})


if __name__ == "__main__":
    app.run()