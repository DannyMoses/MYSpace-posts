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

    data = request.json
    print("USER:", data["user"])

    x = uuid.uuid1()
    uid = str(x)

    post = {"username": data["user"],
            "content": data["content"],
            "type": data["childType"],
            "property": {
                "likes": '0'
                },
            "retweeted": '0',
            "timestamp": time.time(),
            "id" : uid
            }

    item_collection = mongo.db.items
    try:
        item_collection.insert_one(post)
    except Exception as e:
        print(e)
        return { "status" : "error", "error" : "Contact a developer" }, 200

    return { "status" : "OK", "id" : uid }, 200

@app.route("/item", methods=["GET"])
def get_item():
    item_collection = mongo.db.items
    id = request.args.get('id')
    print(id)

    ret = item_collection.find_one({"id" : id})
    if not ret: 
        return { "status" : "error", "error": "Item not found" }, 200 #400

    del ret['_id']
    return { "status": "OK", "item": ret }, 200

@app.route("/search", methods=["POST"])
def search():
    data = request.json
    item_collection = mongo.db.items
    limit = 25
    if "limit" in data:
        limit = data["limit"]
    if limit > 100:
        limit = 100
    timestamp = time.time()
    if "timestamp" in data:
        timestamp = data["timestamp"]
    ret = item_collection.find({ "timestamp" : { '$lt' : timestamp } }).limit(limit)

    if not ret:
        return { "status" : "error", "error": "No items found" }, 200 #400

    results = []
    for doc in ret:
        del doc['_id']
        results.append(doc)
        print(doc)
        #print(ret[i])
        #results.append(ret[i])

    return { "status" : "OK", "items": results }, 200

