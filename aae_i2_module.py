import os
from autumn_db.autumn_db import DBCoreEngine
from autumn_db.autumn_db.network import ClientEndpoint

os.environ['AAE_CONFIG_NAME'] = 'aae_i2_conf'

holder_name = 'test_holder2'
db_core = DBCoreEngine(holder_name)

endpoint = ClientEndpoint(50011, db_core)
endpoint.processing()