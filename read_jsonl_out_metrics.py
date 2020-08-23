import argparse
import logging 
import pandas as pd 
import numpy as np 
import pandasql as ps 
from urllib.parse import urlparse , parse_qs
import json_lines 
import json 
import os 



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
        parser.add_argument('-f', '--input_file_name', action='store',default = "data.json1",
                                           help='JSON FILE  name.')
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
        


def calc_metrics_window(sliced_df , row_count):
    sql = """
    select count(distinct(anonymous_user_id)) as count_distinct_id , 
    min(time) as min_time, 
    max(time) as max_time 
    from sliced_df  
    """
    out = ps.sqldf(sql)
    return out['count_distinct_id'][0], out ['min_time'][0] , out['max_time'][0]
    

    ## MAIN FUNCTION #########

# ###########  GLOBAL Variables  ###############
SetupLogging()
v_input_file_name= main()
if v_input_file_name is None  :
    logging.error("cannot open input  file",exc_info=True)
      
#v_input_file_name = "data.json1"

cols = ['anonymous_user_id', 'time']
jsonl_df = pd.DataFrame(columns = cols )
#print(type(jsonl_df))

    
try:
    with open(v_input_file_name, 'rb') as input_file:
        row_count = 0 
        # Read one line at a time 
        for line in json_lines.reader(input_file):
            new_row = {'anonymous_user_id' : line['anonymous_user_id'], 'time': pd.to_datetime((line['time']),unit='ns')}
            jsonl_df = jsonl_df.append(new_row, ignore_index=True)
            row_count = row_count + 1 

            if (row_count%1000 == 0):
                #break 
                # Creating a Window of 1000 records 
                v_end = row_count
                v_start = row_count - 1000
                sliced_df = jsonl_df[v_start:v_end]
                count_distinct_id, min_time , max_time = calc_metrics_window(sliced_df, row_count)
                final_metrics = {'count_distinct_id':str(count_distinct_id) ,'min_time':str(min_time), 'max_time':str(max_time) }
                #print (count_distinct_id,min_time,max_time  )
                v_append_str = str(int(row_count/1000))
                metrics_json = json.dumps(final_metrics)
                out_file_name = v_append_str+".json"
                outfile = open(out_file_name , "w")
                outfile.write(metrics_json)

except FileNotFoundError:
    print('File does not exist')
except :
    logging.error(sys.exc_info()[0]) 
   

