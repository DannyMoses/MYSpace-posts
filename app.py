from flask import Flask, session, request, render_template, make_response
from werkzeug.utils import secure_filename
from flask_pymongo import PyMongo
import boto3
import requests
from pprint import pprint

from datetime import date
import logging, time, json, uuid, os

from config import config

app = Flask(__name__)
app.config['MONGO_URI'] = "mongodb://{}:{}@{}/{}".format(
				config['mongo_usr'],
				config['mongo_pwd'],
				config['mongo_ip'],
				config['mongo_db']
			)
mongo = PyMongo(app)

ceph = boto3.resource('s3',
        use_ssl = False,
	endpoint_url = 'http://' + config['ceph_ip'],
	aws_access_key_id = config['ceph_access_key'],
	aws_secret_access_key = config['ceph_secret_key']
)
media_store = ceph.Bucket('media')
bucket_resp = media_store.create()
print(bucket_resp)
app.config["UPLOAD_FOLDER"] = config['upload_folder']

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

	item_collection = mongo.db.items

	if 'childType' not in data:
		data['childType'] = None
	if 'parent' not in data:
		data['parent'] = None
	if 'media' not in data:
		data['media'] = []
	else:
		print("Media array: ", data['media'])
		mongo_ret = item_collection.find_one({'media': { '$in': data['media'] } } )
		print(mongo_ret)
		if mongo_ret:
			return { "status" : "error", "error": "Media items in use" }, 200 #400

		r = requests.post(url=('http://' + profiles_route + '/user_media'), json={'user': data['user']})
		#print(r)
		#print(r.text)
		r_json = r.json()
		for x in data['media']:
			if x not in r_json['user_media']:
				return { "status" : "error", "error": "Not media item owner" }, 200 #400

		query = {
			"query": {
				"terms": {"media": data['media'] }
			}
		}

		app.logger.info(query)

		r = requests.get(url=('http://' + search_route + '/posts/_search'), json=query)
		r_json = r.json()
		#print(r_json)
		#print(r_json['hits'])
		app.logger.debug(r_json['hits']['hits'])
		print(r_json['hits']['total'])

		if r_json['hits']['total']['value'] > 0:
			return { "status" : "error", "error": "Media items in use" }, 200 #400

	post = {
			"id" : uid,
			"username": data['user'],
			"property": {
				"likes": 0
			},
                        "liked_by": [],
			"retweeted": 0,
			"content": data['content'],
			"timestamp": int(round(time.time()*1000)),
			"childType": data['childType'],
			"parent": data['parent'],
			"media": data['media']
		}

	try:
		item_collection.insert_one(post)
	except Exception as e:
		print(e)
		return { "status" : "error", "error" : "Contact a developer" }, 200

	# Retweet
	if data['childType'] == "retweet":
		item_collection.update_one({'id': data['parent']}, {'$inc': {'retweeted': 1}})
		r = requests.post(url=('http://' + search_route + '/posts/_update/' + data['parent']),
				json={
					"script": {
						"source": "ctx._source.interest += params.count",
						"params": { "count": 1 }
					}
				})
		print(r)

	del post['_id']
	del post['id']
	del post['property']
	del post['liked_by']
	del post['retweeted']
	post['isReply'] = True if post['childType'] == "reply" else False
	del post['childType']
	post['interest'] = 0

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
	print("delete /item")
	item_collection = mongo.db.items
	content = request.json
	app.logger.debug(content)
	print(content)

	# Check item exists
	item = item_collection.find_one({"id" : content['id']})
	if not item:
		print('Item not found')
		return { "status" : "error", "error": "Item not found" }, 404
	if item['username'] != content['user']:
		print('Not item creator')
		return { "status" : "error", "error": "Not item creator" }, 403

	print("item exists")

	# Delete item from mongo
	ret = item_collection.delete_one({"id" : content['id']})
	if ret.deleted_count == 0:
		print('Mongodb item not deleted')
		return { "status" : "error", "error": "Item not deleted successfully" }, 404

	print("mongodb passed")

	# Delete item from elasticsearch
	r = requests.delete(url=('http://' + search_route + '/posts/_doc/' + content['id']))
	print("EL DELETE CODE:", r.status_code)

	r_json = r.json()
	app.logger.debug(r_json)
	print(r_json)

	# Delete media
	del_objs = [{'Key': x} for x in item['media']]
	print("del_objs:", del_objs)
	print("list built")
	ret = media_store.delete_objects(Delete={'Objects': del_objs})
	print("ceph deleted objects")
	app.logger.debug(ret)
	r = requests.delete(url=('http://' + profiles_route + '/user/media'), json={'user': content['user'], 'media': item['media']})

	print("Delete complete")
	print("EL CODE:", r.status_code)

	del item['_id']
	app.logger.debug(item)
	print(item)
	return { "status": "OK" }, 200

@app.route("/item/like", methods=["POST"])
def like_item():
	print("/item/like")
	item_collection = mongo.db.items
	content = request.json
	app.logger.debug(content)
	print(content)

	ret = None
	inc = 0
	if content['like']:
		ret = item_collection.update_one(
			{"id" : content['id']},
			{'$addToSet': {"liked_by": content['user'] }}
		)
		#print(ret)
		if ret.acknowledged and ret.modified_count:
			inc = 1
	else:
		ret = item_collection.update_one(
			{"id" : content['id']},
			{'$pull': {"liked_by": content['user'] }}
		)
		#print(ret)
		if ret.acknowledged and ret.modified_count:
			inc = -1

	if inc:
		ret = item_collection.update_one(
			{"id" : content['id']},
			{'$inc': {"property.likes": inc }}
		)
		r = requests.post(url=('http://' + search_route + '/posts/_update/' + content['id']),
					json={
						"script": {
							"source": "ctx._source.interest += params.count",
							"params": { "count": inc }
						}
					})
		#print("like", r.text)

	if ret.acknowledged and ret.modified_count:
		return { "status": "OK" }, 200
	else:
		return { "status" : "error", "error": "Already un/liked, or the item does not exist" }, 200 #400

@app.route("/search", methods=["POST"])
def search():
	data = request.json
	item_collection = mongo.db.items
	
	app.logger.debug(data)
	print(data)

	# Limit defaults
	limit = 25
	if "limit" in data:
		limit = data["limit"]
	if limit > 100:
		limit = 100

	# Setup search query
	search = []
	filter = []
	#fields = ['username', 'timestamp', 'interest']

	# Time defaults
	timestamp = int(round(time.time()*1000))
	if "timestamp" in data:
		timestamp = data["timestamp"]
	filter.append({ "range": {"timestamp": {"lte": timestamp}} })

	# String query
	if "q" in data and data['q']:
		search.append({ "match": {"content": data['q']} })
		#fields.append(content)

	# By username or followed users
	if 'username' in data:
		filter.append({ "term": {"username": data['username']} })
	elif 'user' in data:
		r = requests.post(url=('http://' + profiles_route + '/user/following'),
				json={'username': data['user']})
		r_json = r.json()
		filter.append({ "terms": {"username": r_json['users']} })

	# Exclude replies
	if 'replies' in data and not data['replies']:
		filter.append({ "term": {"isReply": False} })
	# Children of parent only
	elif 'parent' in data:
		filter.append({ "term": {"parent": data['parent']} })
	# has media
	if 'hasMedia' in data and data['hasMedia']:
		filter.append({"exists": {'field': "media"}})
		#fields.append('media')

	query = {
		"_source": ["username", "timestamp", "interest"],
		"query": {
			"bool": {
				"filter": filter
			}
		},
		"sort": [{"interest": "desc"}],
		"size": limit
	}

	if search:
		query['query']['bool']['must'] = search

	# Rank
	if 'rank' in data and data['rank'] == "time":
		app.logger.debug("Sorting by time")
		query['sort'] = [{"timestamp": "desc"}]

	app.logger.info(query)

	r = requests.get(url=('http://' + search_route + '/posts/_search'), json=query)
	r_json = r.json()
	#print(r_json)
	#print(r_json['hits'])
	app.logger.debug(r_json['hits']['hits'])
	print(r_json['hits']['total'])

#	if r_json['hits']['total']['value'] == 0:
#		return { "status" : "error", "error": "No items found" }, 200 #400

	results = []
	for search_result in r_json['hits']['hits']:
		mongo_ret = item_collection.find_one({"id": search_result['_id']})
		#app.logger.debug(mongo_ret)
		del mongo_ret['_id']
		results.append(mongo_ret)
		#print(ret[i])
		#results.append(ret[i])
	print(80*'=')
	#pprint(results)
	return { "status" : "OK", "items": results }, 200

@app.route("/addmedia", methods=["POST"])
def add_media():
	print("/ADDMEDIA() CALLED")
	print("TRYING TO OPEN FILE")
	media_file = request.files['content']
	print("OPENED FILE")

	print("data: ", request.form)

	print(media_file)
	print(media_file.headers)
	#print(media_file.read())

#	if 'Content-Type' not in media_file.headers:
#		return { "status" : "error", "error": "No file provided" }, 200

	filename = secure_filename(media_file.filename)
	#file_ext = os.path.splitext(filename)
	id = uuid.uuid1()
	media_id = str(id) + "-" + filename

	print(filename)
	print(media_id)

	media_store.upload_fileobj(
		media_file, media_id
		#ExtraArgs={'ContentType': media_file.headers['Content-Type']}
	)

	r = requests.post(url=('http://' + profiles_route + '/add_media'), json={'user': request.form['user'], 'media_id': media_id})
	print(r)
	print(r.text)

	return { "status" : "OK", "id": media_id }, 200

@app.route("/media", methods=["GET"])
def get_media():
	media_id = request.args.get('id')
	app.logger.debug(media_id)
	id = uuid.uuid1()
	temp_file = app.config['UPLOAD_FOLDER'] + str(id)

	response = None
	with open(temp_file, 'wb') as f:
		media_store.download_fileobj(media_id, f)
		print(f)
		f.close()
	
	with open(temp_file, 'rb') as f:
		#print(f.read())
		response = make_response(f.read())
		print("added blob to response")
		#response.headers.set('Content-Type', mimetype)
		#response.headers.set('Content-Disposition', 'attachment', filename=filename)
		f.close()

	os.remove(temp_file)

	return response, 200 

	return { "status" : "OK" }, 200

@app.route("/media", methods=["DELETE"])
def delete_media():
	data = request.json
	media_obj = media_store.Object(data['id'])
	ret = media_obj.delete()
	app.logger.debug(ret)
	r = requests.delete(url=('http://' + profiles_route + '/user/media'), json={'user': data['user'], 'media': [media_id]})
	return { "status" : "OK" }, 200

