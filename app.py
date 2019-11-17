from flask import Flask, session, request, render_template
from flask_pymongo import PyMongo
import requests

from datetime import date
import logging, time, json, uuid

from config import config

app = Flask(__name__)
app.config['MONGO_URI'] = "mongodb://{}:{}@{}/{}".format(
				config['mongo_usr'],
				config['mongo_pwd'],
				config['mongo_ip'],
				config['mongo_db']
			)

mongo = PyMongo(app)

search_route = config["elasticsearch_route"]
profiles_route = config["profiles_route"]

# Setup logging
if __name__ != '__main__':
	gunicorn_logger = logging.getLogger('gunicorn.error')
	app.logger.handlers = gunicorn_logger.handlers
	app.logger.setLevel(gunicorn_logger.level)

@app.route("/reset_posts", methods=["POST"])
def reset():
	query = {
		"query": {
			"match_all" : {}
		}
	}

	mongo.db.items.drop()
	requests.post(url=('http://' + search_route + '/posts/_delete_by_query'), json=query)
	return { "status": "OK" }, 200

@app.route("/additem", methods=["POST"])
def add_item():
	app.logger.debug(80*'=')
	app.logger.debug("/ADDITEM()")

	data = request.json
	#app.logger.info("USER:", data["user"])

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
	content = request.json
	app.logger.debug(content)

	item = item_collection.find_one({"id" : content['id']})
	if not item:
		return { "status" : "error", "error": "Item not found" }, 404
	if item['username'] != content['user']:
		return { "status" : "error", "error": "Not item creator" }, 403

	ret = item_collection.delete_one({"id" : content['id']})
	if ret.deleted_count == 0:
		return { "status" : "error", "error": "Item not deleted successfully" }, 404

	r = requests.delete(url=('http://' + search_route + '/posts/_doc/' + content['id']))

	r_json = r.json()
	app.logger.debug(r_json)

	del item['_id']
	app.logger.debug(item)
	return { "status": "OK", "item": item}, 200

@app.route("/search", methods=["POST"])
def search():
	data = request.json
	item_collection = mongo.db.items
	
	app.logger.debug(data)

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

	if "q" in data and data['q']:
		search.append({ "match": {"content": data['q']} })
	if 'username' in data:
		filter.append({ "term": {"username": data['username']} })
	elif 'user' in data:
		r = requests.post(url=('http://' + profiles_route + '/user/following'),
				json={'username': data['user']})
		r_json = r.json()
		filter.append({ "terms": {"username": r_json['users']} })
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

	app.logger.info(query)

	r = requests.get(url=('http://' + search_route + '/posts/_search'), json=query)
	r_json = r.json()
	#print(r_json['hits'])
	app.logger.debug(r_json['hits']['hits'])
	#print(r_json['hits']['total'])

#	if r_json['hits']['total']['value'] == 0:
#		return { "status" : "error", "error": "No items found" }, 200 #400

	results = []
	for search_result in r_json['hits']['hits']:
		mongo_ret = item_collection.find_one({"id": search_result['_id']})
		app.logger.debug(mongo_ret)
		del mongo_ret['_id']
		results.append(mongo_ret)
		#print(ret[i])
		#results.append(ret[i])

	return { "status" : "OK", "items": results }, 200

