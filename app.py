from flask import Flask, session, request, render_template
from flask_pymongo import PyMongo
from datetime import date
import time
import json
import uuid

app = Flask(__name__)
app.config['MONGO_URI'] = "mongodb://localhost:27017/posts"

mongo = PyMongo(app)

@app.route("/additem", methods=["POST"])
def add_item():
	print(80*'=')
	print("/ADDITEM()")
	print("USER:", session["user"])
	
	x = uuid.uuid1()
	uid = str(x)
	data = request.json
	post = { "username" : data["user"],
		"content" : data["content"],
		"type": data["childType"],
		"property" : {
			"likes" : '0'
		},
		"retweeted" : '0',
		"timestamp" : str(time.time()),
		"id" : uid
		}

	item_collection = mongo.db.items
	item_collection.insert_one(post)

	return { "status" : "OK", "id" : uid }, 200

@app.route("/item/<id>", methods=["GET"])
def get_item(id):
	item_collection = mongo.db.items

	data = request.json

	ret = item_collection.find_one({"id" : data["id"]})

	if ret is None: 
		return { "status" : "ERROR" }, 200 #400
	
	return ret, 200

@app.route("/search", methods=["POST"])
def search():
	data = request.json
	item_collection = mongo.db.items
	ret = item_collection.find({ "timestamp" : { '$lt' : data["timestamp"] } }).limit(data['limit'])

	return { "status" : "OK", items: ret }, 200


