from flask import Flask, session, request, render_template, make_response
from werkzeug.utils import secure_filename
from flask_pymongo import PyMongo
import boto3, swiftclient
from botocore.exceptions import ClientError
import requests

from datetime import date
import logging, time, json, uuid, os

from config import config
from pprint import pprint

app = Flask(__name__)

# MongoDB
app.config['MONGO_URI'] = "mongodb://{}:{}@{}/{}".format(
				config['mongo_usr'],
				config['mongo_pwd'],
				config['mongo_ip'],
				config['mongo_db']
			)
mongo = PyMongo(app)

# Ceph Swift
ceph_swift = swiftclient.client.Connection(
	user = config['ceph_swift_user'],
	key = config['ceph_swift_secret_key'],
	authurl = 'http://' + config['ceph_ip'] + '/auth'
)
media_container = "media"
ceph_swift.put_container(media_container)
# Ceph s3 (for /reset_posts)
ceph_s3 = None
media_store = None
try:
	ceph_s3 = boto3.resource('s3',
		use_ssl = False,
		endpoint_url = 'http://' + config['ceph_ip'],
		aws_access_key_id = config['ceph_access_key'],
		aws_secret_access_key = config['ceph_secret_key']
	)
	media_store = ceph_s3.Bucket('media')
	bucket_resp = media_store.create()
	app.logger.info(bucket_resp)
except Exception as ex:
	app.logger.warning("Get ceph resource error")
	app.logger.warning(ex)

# Other Configs
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
	app.logger.warning("/reset_posts called")

	query = {
		"query": {
			"match_all" : {}
		}
	}
	requests.post(url=('http://' + search_route + '/posts/_delete_by_query'), json=query)
	mongo.db.items.drop()
	media_store.objects.all().delete()
	return { "status": "OK" }, 200

@app.route("/additem", methods=["POST"])
def add_item():
	app.logger.debug(80*'=')
	app.logger.debug("/ADDITEM()")

	data = request.json
	app.logger.debug("/additem")
	app.logger.debug(json.dumps(data))

	x = uuid.uuid1()
	uid = str(x)

	item_collection = mongo.db.items

	if 'childType' not in data:
		data['childType'] = None
	if 'parent' not in data:
		data['parent'] = None
	if 'media' not in data:
		data['media'] = []
	elif data['childType'] != "retweet":
		# Check if media already in use
		#print("Media array: ", data['media'])
		app.logger.debug("Media array: {}".format(str(data['media'])))
		mongo_ret = item_collection.find_one({'media': { '$in': data['media'] } } )
		if mongo_ret:
			app.logger.info("/additem media items in use")
			return { "status" : "error", "error": "Media items in use" }, 200 #400

		# Check if media belong to user
		#r = requests.post(url=('http://' + profiles_route + '/user_media'), json={'user': data['user']})
		#print(r)
		#print(r.text)
		#r_json = r.json()
		for x in data['media']:
			media_item = media_store.Object(x)
			try:
				media_item.load()
			except ClientError as e:
				app.logger.debug(e)
				app.logger.info("/additem media item does not exist")
				return { "status" : "error", "error": "Media item does not exist" }, 400

			app.logger.debug(json.dumps(media_item.metadata))
			if media_item.metadata['user'] != data['user']:
				app.logger.info("/additem not media item owner")
				return { "status" : "error", "error": "Not media item owner" }, 200 #400

		query = {
			"query": {
				"terms": {"media": data['media'] }
			}
		}

		app.logger.debug(query)

		# Check if media already in use
		print("Media array: ", data['media'])
		r = requests.get(url=('http://' + search_route + '/posts/_search'), json=query)
		r_json = r.json()
		#print(r_json)
		#print(r_json['hits'])
		app.logger.debug(r_json['hits']['hits'])
		print(r_json['hits']['total'])

		if r_json['hits']['total']['value'] > 0:
			app.logger.info("/additem media item in use")
			return { "status" : "error", "error": "Media item in use" }, 200 #400

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
		app.logger.debug(e)
		app.logger.error("/additem insert post in mongodb error")
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

	# Insert post into elasticsearch
	del post['_id']
	del post['id']
	del post['liked_by']
	post['isReply'] = True if post['childType'] == "reply" else False
	del post['childType']
	post['interest'] = post['property']['likes'] + post['retweeted']
	del post['property']
	del post['retweeted']

	app.logger.debug(post)
	r = requests.put(url=('http://' + search_route + '/posts/_doc/' + uid), json=post)

	r_json = r.json()
	app.logger.debug(r_json)

	# Update media metadata
	for media_id in data['media']:
		media_item = media_store.Object(media_id)
		media_item.load()
		media_item.metadata['references'] = str(int(media_item.metadata['references']) + 1) # increment str counter
		media_item.copy_from(CopySource={'Bucket': 'media', 'Key': media_id}, Metadata=media_item.metadata, MetadataDirective='REPLACE')
		# Cancel potential expiration
		headers = {
			'X-Object-Meta-user': media_item.metadata['user'],
			'X-Object-Meta-references': media_item.metadata['references'],
			'Content-Type': media_item.content_type,
			'X-Remove-Delete-At': True
		}
		r = {}
		ceph_swift.post_object(media_container, media_id, headers, response_dict=r)
		app.logger.debug(r)

	app.logger.info("/additem OK")
	return { "status" : "OK", "id" : uid }, 200

@app.route("/item", methods=["GET"])
def get_item():
	item_collection = mongo.db.items
	id = request.args.get('id')
	app.logger.debug(id)

	ret = item_collection.find_one({"id" : id})
	if not ret: 
		app.logger.info("GET /item item not found")
		return { "status" : "error", "error": "Item not found" }, 200 #400

	del ret['_id']
	app.logger.info("GET /item OK")
	return { "status": "OK", "item": ret }, 200

@app.route("/item", methods=["DELETE"])
def delete_item():
	item_collection = mongo.db.items
	content = request.json
	app.logger.debug("DELETE /item data: {}".format(content))

	# Check item exists
	item = item_collection.find_one({"id" : content['id']})
	if not item:
		app.logger.info("DELETE /item item not found")
		return { "status" : "error", "error": "Item not found" }, 404
	if item['username'] != content['user']:
		app.logger.info("DELETE /item not item creator")
		return { "status" : "error", "error": "Not item creator" }, 403

	app.logger.debug("DELETE /item item exists")

	# Delete item from mongo
	ret = item_collection.delete_one({"id" : content['id']})
	if ret.deleted_count == 0:
		app.logger.error("DELETE /item MongoDB error")
		return { "status" : "error", "error": "Item not deleted successfully" }, 500

	app.logger.debug("DELETE /item mongodb passed")

	# Delete item from elasticsearch
	r = requests.delete(url=('http://' + search_route + '/posts/_doc/' + content['id']))
	app.logger.debug("EL DELETE CODE:", r.status_code)
	if r.status_code > 299:
		app.logger.error("DELETE /item elasticsearch error")
		app.logger.debug("DELETE /item elasticsearch delete: {}".format(r.json()))
		return { "status" : "error", "error": "Item not deleted successfully" }, 500

	# Delete media
	#del_objs = [{'Key': x} for x in item['media']]
	#app.logger.debug("del_objs:", del_objs)
	#ret = media_store.delete_objects(Delete={'Objects': del_objs})
	#app.logger.debug("ceph deleted objects")
	#app.logger.debug(ret)
	#r = requests.delete(url=('http://' + profiles_route + '/user/media'), json={'user': content['user'], 'media': item['media']})

	# Update media metadata, deleting if this item held last reference to the media item
	for x in item['media']:
		media_item = media_store.Object(x)
		media_item.load()
		app.logger.debug(media_item.metadata)
		if int(media_item.metadata['references']) <= 1:
			r = media_item.delete()
			app.logger.debug("ceph deleted a media object")
			app.logger.debug(r)
		else:
			media_item.metadata['references'] = str(int(media_item.metadata['references']) - 1) # increment str counter
			media_item.copy_from(CopySource={'Bucket': 'media', 'Key': x}, Metadata=media_item.metadata, MetadataDirective='REPLACE')

	#app.logger.debug("Delete complete")
	#app.logger.debug("EL CODE:", r.status_code)

	del item['_id']
	app.logger.debug(item)
	return { "status": "OK" }, 200

@app.route("/item/like", methods=["POST"])
def like_item():
	item_collection = mongo.db.items
	content = request.json
	app.logger.debug("/item/like data: {}".format(content))

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
		app.logger.info("/item/like OK")
		return { "status": "OK" }, 200
	else:
		app.logger.info("/item/like error")
		return { "status" : "error", "error": "Already un/liked, or the item does not exist" }, 200 #400

@app.route("/search", methods=["POST"])
def search():
	data = request.json
	item_collection = mongo.db.items
	app.logger.debug("/search data: {}".format(data))

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
	timestamp = time.time()
	if "timestamp" in data:
		timestamp = data["timestamp"]
	timestamp = int(round(timestamp*1000))
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

	app.logger.debug("/search query: {}".format(query))

	r = requests.get(url=('http://' + search_route + '/posts/_search'), json=query)
	r_json = r.json()
	app.logger.debug(r_json)
	#print(r_json)
	#print(r_json['hits'])
	app.logger.debug(r_json['hits']['hits'])
	app.logger.debug(r_json['hits']['total'])
	#print(r_json['hits']['total'])

#	if r_json['hits']['total']['value'] == 0:
#		return { "status" : "error", "error": "No items found" }, 200 #400

	results = []
	for search_result in r_json['hits']['hits']:
		mongo_ret = item_collection.find_one({"id": search_result['_id']})
		if mongo_ret:
			#app.logger.debug(mongo_ret)
			del mongo_ret['_id']
			results.append(mongo_ret)
			#print(ret[i])
			#results.append(ret[i])
	#print(80*'=')
	#pprint(results)
	app.logger.info("/search OK")
	return { "status" : "OK", "items": results }, 200

@app.route("/addmedia", methods=["POST"])
def add_media():
	app.logger.debug("/addmedia data: {}".format(request.form))
	#print("/ADDMEDIA() CALLED")
	#print("TRYING TO OPEN FILE")
	media_file = request.files['content']
	#print("OPENED FILE")
	app.logger.debug("/addmedia file headers: {}".format(media_file.headers))

	#print(media_file)
	#print(media_file.headers)
	#print(media_file.read())

#	if 'Content-Type' not in media_file.headers:
#		return { "status" : "error", "error": "No file provided" }, 200

	filename = secure_filename(media_file.filename)
	#file_ext = os.path.splitext(filename)
	id = uuid.uuid1()
	media_id = str(id) + "-" + filename

	#print(filename)
	#print(media_id)

	extra_args = {'Metadata': {'user': request.form['user'], 'references': "0"}}
	#if 'Content-Type' in media_file.headers:
	#	extra_args['ContentType'] = media_file.headers['Content-Type']

	# Add media to ceph
	media_store.upload_fileobj(
		media_file, media_id,
		ExtraArgs=extra_args
	)

	#o = media_store.Object(media_id)
	#o.load()
	#app.logger.debug('/addmedia media added via s3 {}'.format(o.metadata))

	# Set headers for object expiration in case media isn't used
	# Object expires in 120 seconds
	headers = {
		'X-Object-Meta-user': request.form['user'],
		'X-Object-Meta-references': "0",
		#'Content-Type': media_file.headers['Content-Type'],
		'X-Delete-At': int(time.time()) + 120
	}
	#if 'Content-Type' in media_file.headers:
	#	headers['Content-Type'] = media_file.headers['Content-Type']
	r = {}
	ceph_swift.post_object(media_container, media_id, headers, response_dict=r)
	app.logger.debug(json.dumps(r))

	#o = media_store.Object(media_id)
	#o.load()
	#app.logger.debug('/addmedia media expire added via swift {}'.format(o.metadata))

	#r = requests.post(url=('http://' + profiles_route + '/add_media'), json={'user': request.form['user'], 'media_id': media_id})
	#print(r)
	#print(r.text)

	app.logger.info("/addmedia OK")
	return { "status" : "OK", "id": media_id }, 200

@app.route("/media", methods=["GET"])
def get_media():
	media_id = request.args.get('id')
	app.logger.debug(media_id)
	id = uuid.uuid1()
	temp_file = app.config['UPLOAD_FOLDER'] + str(id)

	# TODO: Streaming?
	response = None
	file_obj = None
	with open(temp_file, 'wb') as f:
		try:
			file_obj = media_store.Object(media_id)
			file_obj.load()
			file_obj.download_fileobj(f)
		#print(f)
		except ClientError as e:
			app.logger.debug(e)
			app.logger.info("GET /media issue retrieving file")
			return { "status" : "error", "error" : "Does this file still exist?" }, 404
		finally:
			f.close()

	with open(temp_file, 'rb') as f:
		#print(f.read())
		response = make_response(f.read())
		#print("added blob to response")
		#response.headers.set('Content-Type', file_obj.content_type)
		#response.headers.set('Content-Disposition', 'attachment', filename=filename)
		f.close()

	os.remove(temp_file)

	app.logger.info("GET /media OK")
	return response, 200 
	#return { "status" : "OK" }, 200

@app.route("/media", methods=["DELETE"])
def delete_media():
	data = request.json
	media_obj = media_store.Object(data['id'])
	ret = media_obj.delete()
	app.logger.debug(ret)
	#r = requests.delete(url=('http://' + profiles_route + '/user/media'), json={'user': data['user'], 'media': [media_id]})
	app.logger.info("DELETE /media OK")
	return { "status" : "OK" }, 200

