# ENVIRONMENT : Python 3.7 
# Operating SYSTEM : Linux 
# Log FIle name = create_jsonl.log (will be created in the same directory)
# User will need write access to the directory in which the script is run 

# Libraries can be installed using the command in OS.
# pip install pandas  numpy pandasql urllib json json_lines
# or pip install -r requirements.txt 



import argparse
import logging 
import pandas as pd 
import numpy as np 
#!pip install pandasql
import pandasql as ps 
from urllib.parse import urlparse , parse_qs
#from pyspark.sql import sparkSession
#!pip install json_lines 
import json_lines 
import json 


def main():
    v_input_file_name = ""
    v_args = GetArgs() 
    v_input_file_name=v_args.input_file_name
    return v_input_file_name 


def GetArgs():
        """
        Supports the command-line arguments listed below.
        """
        parser = argparse.ArgumentParser(
           description='Reads 1 arguments  -f (file name)')
        parser.add_argument('-f', '--input_file_name', action='store',default = "data.csv",
                                           help='CSV FILE name.')
        args = parser.parse_args()
      # print (args)
        return args 
        

    

def SetupLogging():
    # Log file name : create_jsonl.log (File gets created in the same directory)
    try:
        debug = 0
        if debug == 1 :
           info = logging.DEBUG
        else :
           info = logging.INFO
        logger = logging.getLogger()
        logger.setLevel(info)
        #create a file handler
        handler = logging.FileHandler('create_jsonl.log')
        handler.setLevel(info)
        # create a logging format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        # add the handlers to the logger
        logger.addHandler(handler)
    except IOError:
        logger.error("cannot open log file",exc_info=True)
        

        
def get_medium(x):
    
    try:
        if v_medium in parse_qs(urlparse(x['url']).query).keys():
            v_medium_list = parse_qs(urlparse(x['url']).query)['utm_medium']
            v_medium_set = set(v_medium_list)
        #y=0
        #if len(v_medium_set) >1 :
        #    y=y+1 
        #return y     
        # Max value of the list was 2 and was duplicating the same value as Medium 
        #Eg. Multiple "email" as set as Medium . Return value is the first occurance 
            return (v_medium_list[0])
        
        else: 
            return None 
    except:
        logging.error(sys.exc_info()[0])
        
    
def get_source(x):
    try:
        if v_source in parse_qs(urlparse(x['url']).query).keys():
            v_source_list = parse_qs(urlparse(x['url']).query)['utm_source']
            v_source_set = set (v_source_list)
            v_uniq_source_list = list(v_source_set)
            v_ret = None 
            for i in range (len(v_uniq_source_list)):
                if i == 0:
                    v_ret = v_uniq_source_list[0]
                else:
                    v_ret = v_ret+ "---" +v_uniq_source_list[i]
            return v_ret 
        
            
        else: 
            return None 
    except:
        logging.error(sys.exc_info()[0]) 
    
def get_path(x):
    if 1:
        return urlparse(x['url']).path
    



## MAIN FUNCTION #########

# ###########  GLOBAL Variables  ###############
SetupLogging()
v_input_file_name= main()
if v_input_file_name is None  :
    logging.error("cannot open input  file",exc_info=True)
      
#v_input_file_name = "data.csv"
source_df = pd.read_csv(v_input_file_name)

v_medium = 'utm_medium'
v_source = 'utm_source'

########## End Global Variables ##########################

##### Applying Transformation #############################

# URL : [protocol:][//host[:port]][path][?query][#fragment]
# URL parameters start after the `?` in the URL and are separated by the `&` character.
# Add 3 new columns to the dataframe 

source_df['time']=(pd.to_datetime(source_df['time'],unit='s')) 
source_df['utm_medium'] = source_df.apply(get_medium, axis =1)
source_df['utm_source'] = source_df.apply(get_source, axis =1)
source_df['path'] = source_df.apply(get_path, axis=1)
#source_df.head()

sql = """select 
anonymous_user_id,url,time,browser,os,screen_resolution,utm_medium,
ltrim(utm_source,'---') as utm_source ,
path
from source_df """

final_df = ps.sqldf(sql)
#final_df.head()

# Save as JSONL . Output file name : data.json1

outfile = open("data.json1", "w")
for row in final_df.iterrows():
    #print(row)
    row[1].to_json(outfile)
    outfile.write("\n")
   
