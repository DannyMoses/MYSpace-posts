from flask import Flask, session, request, render_template
from flask_pymongo import PyMongo
import requests

from datetime import date
import logging, time, json, uuid

from config import config

app = Flask(__name__)
app.config['MONGO_URI'] = "mongodb://localhost:27017/posts"

mongo = PyMongo(app)

search_route = config["elasticsearch_route"]

# Setup logging
if __name__ != '__main__':
	gunicorn_logger = logging.getLogger('gunicorn.error')
	app.logger.handlers = gunicorn_logger.handlers
	app.logger.setLevel(gunicorn_logger.level)

@app.route("/reset_posts", methods=["POST"])
def reset():
	mongo.db.items.drop()
	return { "status": "OK" }, 200

@app.route("/additem", methods=["POST"])
def add_item():
	app.logger.info(80*'=')
	app.logger.info("/ADDITEM()")

	data = request.json
	app.logger.info("USER:", data["user"])

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

	del post['_id']
	del post['id']
	del post['type']
	del post['property']
	del post['retweeted']

	app.logger.debug(post)

	r = requests.put(url=('http://' + search_route + '/posts/_doc/' + uid), json=post)

	r_json = r.json()
	app.logger.debug(r_json)

	return { "status" : "OK", "id" : uid }, 200

@app.route("/item", methods=["GET"])
def get_item():
	item_collection = mongo.db.items
	id = request.args.get('id')
	app.logger.debug(id)

	ret = item_collection.find_one({"id" : id})
	if not ret: 
		return { "status" : "error", "error": "Item not found" }, 200 #400

	del ret['_id']
	return { "status": "OK", "item": ret }, 200

@app.route("/item", methods=["DELETE"])
def delete_item():
	item_collection = mongo.db.items
	id = request.args.get('id')
	app.logger.debug(id)

	ret = item_collection.delete_one({"id" : id})
	if ret.deleted_count == 0:
		return { "status" : "error", "error": "Item not found" }, 404

	r = requests.delete(url=('http://' + search_route + '/posts/_doc/' + id))

	r_json = r.json()
	app.logger.debug(r_json)

	return { "status": "OK"}, 200

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

	search = []
	filter = []

	if "q" in data:
		search.append({ "match": {"content": data['q']} })
	if 'username' in data:
		filter.append({ "term": {"username": data['username']} })
	filter.append({ "range": {"timestamp": {"lte": timestamp}} })

	query = {
		"query": {
			"bool": {
				"filter": filter
			}
		},
		"size": limit
	}

	if search:
		query['query']['bool']['must'] = search

	r = requests.get(url=('http://' + search_route + '/posts/_search'), json=query)
	r_json = r.json()
	#print(r_json['hits'])
	app.logger.debug(r_json['hits']['hits'])
	#print(r_json['hits']['total'])

	if r_json['hits']['total']['value'] == 0:
		return { "status" : "error", "error": "No items found" }, 200 #400

	results = []
	for search_result in r_json['hits']['hits']:
		mongo_ret = item_collection.find_one({"id": search_result['_id']})
		del mongo_ret['_id']
		results.append(mongo_ret)
		app.logger.debug(mongo_ret)
		#print(ret[i])
		#results.append(ret[i])

	return { "status" : "OK", "items": results }, 200

