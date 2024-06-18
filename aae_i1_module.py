import os
from autumn_db.autumn_db import DBCoreEngine
from autumn_db.autumn_db.network import ClientEndpoint

os.environ['AAE_CONFIG_NAME'] = 'aae_i1_conf'

holder_name = 'test_holder'
db_core = DBCoreEngine(holder_name)

endpoint = ClientEndpoint(50001, db_core)
endpoint.processing()