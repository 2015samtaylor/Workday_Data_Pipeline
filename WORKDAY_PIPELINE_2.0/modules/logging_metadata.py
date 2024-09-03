from datetime import datetime
import socket
import pandas as pd
import os
import sys
from .sql_query_module import *

class JobLogger:
    def __init__(self, process_name, job_name, job_type):
        self.process_name = process_name
        self.job_name = job_name
        self.job_type = job_type
        self.start_time = datetime.now()
        self.logs = []
        
    def get_server_ip(self):
        try:
            hostname = socket.gethostname()
            ip_address = socket.gethostbyname(hostname)
            return ip_address
        except Exception as e:
            return f"Unable to get IP Address: {e}"

    def log_job(self, success_failure):
        server_ip = self.get_server_ip()
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        self.logs.append({
            'process_name': self.process_name,
            'job_name': self.job_name,
            'job_type': self.job_type,
            'server_ip': server_ip,
            'success_failure': success_failure,
            'start_time': self.start_time,
            'end_time': end_time,
            'Duration_sec': duration
        })

    def get_logs(self):
        return pd.DataFrame(self.logs)
    
    def send_frame_to_SQL(self):
        df_log = self.get_logs()
        dtypes, table_cols = SQL_query.get_dtypes('DataTeamSandbox', 'process_log')
        try:
            df_log.to_sql(f'process_log', schema='dbo', con = SQL_query.engine, if_exists = 'append', index = False, dtype=dtypes)
            logging.info('Logs sent to process_log table on 89 server')
        except Exception as e:
            logging.info(f'Unable to send df_log to SQL due to {e}')



# Need to write this frame to SQL dynamically across server, with pre-determined dtypes
# Needs to work in tandem with sql module
# In order for this to work on fail there needs to be a main function everytime, otherwise if it breaks at a certain point there will be no way to relay that frame

# def get_script_environment():
#     try:
#         # Check if running in a .py file
#         script_dir = os.path.dirname(os.path.abspath(__file__))
#         env_type = "python_script"
#     except NameError:
#         # Check if running in an IPython notebook
#         try:
#             if 'ipykernel' in sys.modules:
#                 from IPython import get_ipython
#                 if get_ipython() is not None:
#                     script_dir = os.getcwd()
#                     env_type = "notebook"
#                 else:
#                     raise RuntimeError("Unknown environment")
#             else:
#                 raise RuntimeError("Unknown environment")
#         except ImportError:
#             raise RuntimeError("Unknown environment")

#     return(script_dir, env_type)


# def append_parent_dir_to_path():

#     script_dir, env_type = get_script_environment()

#     # Get the absolute path of the current notebook
#     script_path = os.path.abspath(script_dir)
#     # Get the directory of the notebook
#     script_dir = os.path.dirname(script_path)

#     # Get the parent directory of the notebook directory
#     parent_dir = os.path.abspath(os.path.join(script_dir, os.pardir))
#     # Add the parent directory to sys.path
#     sys.path.append(parent_dir)

#     print("Script directory:", script_dir)
#     print("Parent directory:", parent_dir)

#     logging.info(f'Appended {parent_dir} to system path')

