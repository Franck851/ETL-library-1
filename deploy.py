# from dotenv import load_dotenv
# import os
# # import subprocess
# from subprocess import Popen,PIPE, STDOUT
#
# load_dotenv()
#
# version = {}
# with open("./ETL/__version__.py") as fp:
#     exec(fp.read(), version)
# version = version['__version__']
# DATABRICKS_HOST = os.getenv('DATABRICKS_HOST')
# DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')
#
# proc = Popen(['databricks', 'configure', '--token'], stdout=PIPE, stdin=PIPE, stderr=PIPE)
# proc.communicate(input=bytes(DATABRICKS_HOST + '\n' + DATABRICKS_TOKEN, 'utf-8'))
# dbfs_lib_path = 'dbfs:/ETL_lib/ETL_' + version
#
# lib_name = 'ETL-' + version + '-py3.5.egg'