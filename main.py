import dotenv
import json
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import os
from sqlalchemy import (String,
                        Integer,
                        engine_from_config,
                        select,
                        Column)
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import (sessionmaker, declarative_base)
from flask_cors import CORS, cross_origin

"""
list_kris = [
    {
        "id": "12345",
        "name": "dfg1234"
    },
    {
        "id": "1234",
        "name": "kris"
    },
    {
        "id": "123",
        "name": "krisvam"
    },
    {
        "id": "123123",
        "name": "kroskris123"
    }
]
"""


class User(object):
    def __init__(self, id, name):
        self.id = id
        self.name = name


def userInit(self, id, name):
    self.id = id
    self.name = name


import botocore
import botocore.session
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig

client = botocore.session.get_session().create_client('secretsmanager', os.environ['AWS_REGION'])
cache_config = SecretCacheConfig()
cache = SecretCache(config=cache_config, client=client)

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
    if os.getenv(dbPrefix + "HOST", None) is not None:
        host = os.environ[dbPrefix + 'HOST']
        port = os.environ[dbPrefix + "PORT"]
        database = os.environ[dbPrefix + 'DATABASE']
        config = {
            "sqlalchemy.url": f"postgresql://{username}:{password}@{host}:{port}/{database}",
            "sqlalchemy.echo": False,
        }
        key = host + '_' + port + '_' + database
        base = declarative_base()
        userModel = type('UserModel' + str(idx), (base,), {
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
    user = cValue['userModel'](id=1579, name="test")
    session.add(user)

application = Flask(__name__)
cors = CORS(application)

application.app_context().push()


@application.route('/', methods=['GET', 'POST'])
def index():
    if request.method == "POST":
        user_obj = json.loads(request.data)
        for cKey, cValue in configs.items():
            user = cValue['userModel'](id=int(user_obj.get("id")), name=user_obj.get("name"))
            session.add(user)
        return jsonify({'hello': 'world'})
    else:
        list_objs = []
        for cKey, cValue in configs.items():
            stmt = select(cValue['userModel'].id, cValue['userModel'].name)
            results = session.execute(stmt).all()
            for result in results:
                list_objs.append({'id': str(result.id), 'name': result.name})
        # results = json.dumps([ob.__dict__ for ob in list_objs])
        return jsonify(list_objs)


@application.route('/<tmp_id>', methods=['GET'])
def userDetails(tmp_id):
    if request.method == 'GET':
        id = int(tmp_id)
        if id == 0:
            resp = jsonify({'message': 'No Id to query'})
            resp.status_code = 400
            return resp
        results = {}
        for cKey, cValue in configs.items():
            stmt = select(cValue['userModel']).filter_by(id=id)
            result = session.get(cValue['userModel'], id)
            if result is not None:
                results = {'id': str(result.id), 'name': result.name}
        results = jsonify(results)
        results.status_code = 200
        return results


if __name__ == "__main__":
    application.run(host='0.0.0.0', port=8080)
