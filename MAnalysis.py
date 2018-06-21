
import math, os
import boto
from filechunkio import FileChunkIO
from boto.s3.connection import S3Connection,OrdinaryCallingFormat
import argparse
import json
import requests
import csv, codecs, io
import sys
import datetime
import os
from datetime import datetime, timedelta
import time
from dateutil.rrule import DAILY, rrule, MO, TU, WE, TH, FR
from collections import OrderedDict
import logging
PAGESIZE = 50000
HOSTNAME = "https://mdata.mscience.com/"
endpointForSampleData ="analysis-data"
sectorForSampleData = ""
now = datetime.today()
endDate = now - timedelta(days=30)
startDate = endDate - timedelta(days=1)
actualStartDate = None
actualEndDate = None
output = []
token = None


'''def get_day(enddate):
  if (enddate.weekday()== 6):
       print(endDate)
       actualEndDate = endDate - timedelta(days=1)
       print(endDate1)
       startDate = actualEndDate - timedelta(days=1)
       print(startDate)
  else:
       startDate = endDate - timedelta(days=1)'''
    
logging.basicConfig(filename='MAnalysis.log',level=logging.DEBUG)  	
			
		


def get_token(hostname, endpoint, username, password, verifySSL):
    logging.debug("Inside get_token")
    payload = {'username': username, 'password': password, 'grant_type': "password"}
    url = hostname + "api/v1/auth/token"
    headers = {"Accept": "application/json", "Accept-Language": "en-us", "Audience" : "Any", "Cache-Control": "no-cache", "Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(url, headers=headers, data=payload, verify=False)
    
    if response.status_code == 400:
        print("Error: Incorrect username or password supplied. Please try again.")
	logging.debug("Error: Incorrect username or password supplied. Please try again.")
        sys.exit(1)
    try:
        response.raise_for_status()
	logging.debug(response.raise_for_status())
    except:
        print("Sorry couldn't process request. Please contact Support@Mscience.com for assistance.1")
        sys.exit(1)
    return response.json()['access_token']



def get_data(access_token, hostname, endpoint, sector, subsector, ticker, company, metric, yearNum, quarterNum, metricValue, metricUnit, metricType, pubDate, updateDate, groupNum, companyGroup, brandName, city, zipcode,  pubDateBegin, pubDateEnd, verifySSL, actualStartDate,actualEndDate):
        logging.debug("Inside get_data")
        headers = {"Accept": "application/json", "Accept-Language": "en-us", "Audience" : "Any", "Cache-Control": "no-cache", "Content-Type": "application/x-www-form-urlencoded", 'Authorization': "Bearer " + access_token}
        url = hostname + "api/v1/" + endpoint
        next_page_num = 1
    #first call is for data + pages
        params = {'sector': sector, 'subsector': subsector, 'ticker': ticker, 'company': company, 'metric': metric, 'yearNum': yearNum, 'quarterNum': quarterNum, 'metricValue': metricValue, 'metricUnit': metricUnit, 'metricType': metricType, 'pubDate': pubDate, 'updateDate': updateDate, 'groupNum': groupNum, 'companyGroup': companyGroup, 'brandName': brandName, 'city': city, 'zipcode': zipcode, 'page': next_page_num, 'pageSize': PAGESIZE, 'pubDateBegin': pubDateBegin, 'pubDateEnd': pubDateEnd, 'includePagination': True}
        response = requests.get(url, headers=headers, params = params, verify=False)
        try:
                response.raise_for_status()
		logging.debug(response.raise_for_status())
	except Exception as e:
                print(e)
		logging.debug(e)
                print("Sorry couldn't process request. Please contact Support@Mscience.com for assistance.")
                sys.exit(1)
        dict_response = json.loads(response.text, object_pairs_hook=OrderedDict)
    #output.extend(dict_response['transactions'])
        page_info = dict(dict_response['paginationDetails'])

        if page_info['totalPages']==0:
                endDate1 = endDate - timedelta(days=1)
                actualStartDate = endDate1 - timedelta(days=1)
                actualEndDate = endDate1
                logging.debug(actualStartDate)
                logging.debug(actualEndDate)
                print(actualStartDate)
                print(actualEndDate)           
                op = get_data(token, HOSTNAME, endpointForSampleData, sectorForSampleData, "","","","", "", "", "", "", "", "", "", "", "", "", "", "", actualStartDate, actualEndDate,"False", actualStartDate,actualEndDate)
        else:
                for x in range(1, page_info['totalPages']+1):
                        if not actualStartDate:
                                print("actualStartDate")
                                op = get_datapagination(token, HOSTNAME, endpointForSampleData, sectorForSampleData, "","","","", "", "", "", "", "", "", "", "", "", "", "", "", startDate, endDate,"False",x)
                        else:
                                op = get_datapagination(token, HOSTNAME, endpointForSampleData, sectorForSampleData, "","","","", "", "", "", "", "", "", "", "", "", "", "", "", actualStartDate, actualEndDate,"False",x)
        return op   

def get_datapagination(access_token, hostname, endpoint, sector, subsector, ticker, company, metric, yearNum, quarterNum, metricValue, metricUnit, metricType, pubDate, updateDate, groupNum, companyGroup, brandName, city, zipcode,  pubDateBegin, pubDateEnd, verifySSL,pageNum):
    logging.debug("Inside get_datapagination")
    headers = {"Accept": "application/json", "Accept-Language": "en-us", "Audience" : "Any", "Cache-Control": "no-cache", "Content-Type": "application/x-www-form-urlencoded", 'Authorization': "Bearer " + access_token}

    url = hostname + "api/v1/" + endpoint
    params = {'sector': sector, 'subsector': subsector, 'ticker': ticker, 'company': company, 'metric': metric, 'yearNum': yearNum, 'quarterNum': quarterNum, 'metricValue': metricValue, 'metricUnit': metricUnit, 'metricType': metricType, 'pubDate': pubDate, 'updateDate': updateDate, 'groupNum': groupNum, 'companyGroup': companyGroup, 'brandName': brandName, 'city': city, 'zipcode': zipcode, 'page': pageNum, 'pageSize': PAGESIZE, 'pubDateBegin': pubDateBegin, 'pubDateEnd': pubDateEnd, 'includePagination': True}
       
    response = requests.get(url, headers=headers, params = params, verify=False)
    try:
        response.raise_for_status()
	logging.debug(response.raise_for_status())
    except Exception as e:
	    logging.debug(e)
            print(e)
            print("Sorry couldn't process request. Please contact Support@Mscience.com for assistance.")
            sys.exit(1)
    dict_response = json.loads(response.text, object_pairs_hook=OrderedDict)
    output.extend(dict_response['transactions'])
    return output 
	
def convert_data(raw_data, output_file):
    logging.debug("Inside convert_data")
    with open(output_file, 'w') as mdata_file:
        csvwriter = csv.writer(mdata_file, lineterminator='\n')
        count = 0
        for data in raw_data:
            if count == 0:
                header = data.keys()
                csvwriter.writerow(header)
                count += 1
            try:
                csvwriter.writerow([str(x).replace('\\u', '') for x in data.values()])
            except Exception as e:
                print (data.values())
		logging.debug(e)
                raise e
    filePath = os.path.dirname(os.path.abspath(output_file))
    print('Your data has been saved to %s, you will find it in the current folder.' % output_file)



def upload_to_S3Bucket():
     logging.debug("Inside upload_to_S3Bucket")
     try:
          # Connect to S3
          conn = S3Connection('secret code','secret key',calling_format=OrdinaryCallingFormat())
          b = conn.get_bucket('mscience-test-upload')
          # Get file info
          source_path = os.path.dirname(os.path.abspath("MAnalysis.csv"))+'\MAnalysis.csv'
          source_size = os.stat(source_path).st_size
          # Create a multipart upload request
          mp = b.initiate_multipart_upload(os.path.basename(source_path))
          # Use a chunk size of 50 MiB (feel free to change this)
          chunk_size = 52428800
          chunk_count = int(math.ceil(source_size / float(chunk_size)))

	  # Send the file parts, using FileChunkIO to create a file-like object
          # that points to a certain byte range within the original file. We
	  # set bytes to never exceed the original file size.
	  for i in range(chunk_count):
               offset = chunk_size * i
	       bytes = min(chunk_size, source_size - offset)
	       with FileChunkIO(source_path, 'r', offset=offset,
                                bytes=bytes) as fp:
                    mp.upload_part_from_file(fp, part_num=i + 1)

	  # Finish the upload
	  mp.complete_upload()
	  print("File Uploaded")
	  #if os.path.exists("MDataE-UKConsumer.csv"):
          #os.remove("MDataE-UKConsumer.csv")
     except Exception as e:
          logging.debug(e)
          raise e		

	

if __name__ == '__main__':
    logging.debug("Inside main")
    logging.debug(startDate)
    logging.debug(endDate)
    print(startDate)
    print(endDate)	
    try:
            token = get_token(HOSTNAME,endpointForSampleData, "loginid", "password", "False")
            data = get_data(token, HOSTNAME, endpointForSampleData, sectorForSampleData, "","","","", "", "", "", "", "", "", "", "", "", "", "", "", startDate, endDate,"False",None,None)
            convert_data(data, "MAnalysis.csv")
            upload_to_S3Bucket()
    except Exception as e:
         logging.debug(e)
