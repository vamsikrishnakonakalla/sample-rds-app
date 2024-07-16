import dotenv
import json
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import os
from sqlalchemy import (String,
                        Integer,
                        engine_from_config,
                        Column)
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import (sessionmaker, declarative_base)

      
def get_instance_region():
  import requests
  instance_identity_url = "http://169.254.169.254/latest/dynamic/instance-identity/document"
  session = requests.Session()
  r = requests.get(instance_identity_url)
  response_json = r.json()
  region = response_json.get("region")
  return(region)

def userInit(self, id, name):
  self.id = id
  self.name = name

import botocore 
import botocore.session 
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig 

client = botocore.session.get_session().create_client('secretsmanager', get_instance_region())
cache_config = SecretCacheConfig()
cache = SecretCache( config = cache_config, client = client)

secret_arn = os.environ['RDS_SECRETMANAGER_ARN']
username_key = os.environ['RDS_DB_USERNAME']
password_key = os.environ['RDS_DB_PASSWORD']

secret = json.loads(cache.get_secret_string(secret_arn))
username = secret[username_key]
password = secret[password_key]

engine_type_prefixes = [
  "AURORA_MYSQL_",
  "AURORA_POSTGRESQL_",
  "RDS_MYSQL_",
  "RDS_MARIADB_",
  "RDS_POSTGRESQL_",
  "RDS_ORACLE_",
  "RDS_SQLSERVER_",
  "RDS_DB2_"
]

configs = {}
binds = {}
dotenv.load_dotenv()

for idx, dbPrefix in enumerate(engine_type_prefixes):
  if os.getenv(dbPrefix+"HOST", None) is not None:
    host = os.environ[dbPrefix+'HOST']
    port = os.environ[dbPrefix+"PORT"]
    database = os.environ[dbPrefix+'DATABASE']
    config = {
      "sqlalchemy.url": f"postgresql://{username}:{password}@{host}:{port}/{database}",
      "sqlalchemy.echo": False,
    }
    key = host+'_'+port+'_'+database
    base = declarative_base()
    userModel = type('UserModel'+str(idx), (base,), {
      "__tablename__": 'userdata',
      "id": Column(Integer, primary_key=True),
      "name": Column(String()),
      "__init__": userInit
    })
    engine = engine_from_config(config)
    if not database_exists(engine.url): create_database(engine.url)
    base.metadata.drop_all(bind=engine)
    base.metadata.create_all(bind=engine)
    config = {
      'userModel': userModel,
      'engine': engine
    }
    binds[userModel] = engine
    configs[key] = config

Session = sessionmaker(twophase=True)
Session.configure(binds=binds)
session = Session()

for cKey, cValue in configs.items():
  user = cValue['userModel'](id=1, name="test")
  session.add(user)

application = Flask(__name__)

application.app_context().push()

@application.route('/')
def index():
    return jsonify({'hello': 'world'})

@application.route('/user',methods=['GET','POST'])
def userDetails():

    if request.method == 'GET':

       if 'id' not in request.form:
          resp = jsonify({'message' : 'No Id to query'})
          resp.status_code = 400
          return resp
       results = {}
       for cKey, cValue in configs.items():
         result = cValue['userModel'].queryfilter_by(id=request.form.get('id')).first()
         if result is None:
          resp = jsonify({'message' : 'User Not Found'})
           
         resp = jsonify({'id':result.id,'name':result.name})
         results[cKey] = resp
         
       result = UserModel.query.filter_by(id=request.form.get('id')).first()
       
       results.status_code = 200
       return results

if __name__=="__main__":
    application.run()
