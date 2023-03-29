import requests
import json
import re
import mysql.connector
import pandas as pd
import numpy as np
import logging as log
from datetime import datetime, timedelta
from datetime import date
import sys

from sqlalchemy import create_engine
from datetime import datetime, timedelta


url_reservation_by_display_unit= 'https://api.broadsign.com:10889/rest/content/v11?domain_id=17244398';


#print(url_reservation_container)
s=requests.get(url_reservation_by_display_unit,auth=("inifinia","infinia2022"), verify='merged_cert.pem')
data=json.loads(s.text)

print(data)
