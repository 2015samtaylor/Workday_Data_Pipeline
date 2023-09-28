import requests
import xmltodict
import json
import logging




class Data_Request:
    
    def __init__(self, user, password):
        self.user = user
        self.password = password
    
# ---------------------------------method to authenticate and get a report--------------------------------

    def get_report(self, link):
        
        session = requests.Session()
        session.auth = (self.user, self.password)
        
        auth = session.post(link)
        response = session.get(link)
        
        if response.status_code == 200:
            logging.info('{} \n status_code-{}\n'.format(link,response.status_code))
            doc = xmltodict.parse(response.content)
            doc = json.dumps(doc)
            doc = json.loads(doc)
            doc = doc['wd:Report_Data']['wd:Report_Entry']
            return(doc)
        else:
            raise Exception(logging.info('Failed to get Workday Reports'))