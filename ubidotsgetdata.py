import json
import requests
import time
import random
from datetime import datetime
import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode

#define the token, device label and variable labels
token = "35characterstoken"
device_label = "ujiolob"
var_label1 = "bat-charge"
var_label2 = "input-frequency"
var_label3 = "input-voltage"
var_label4 = "output-voltage"
var_label5 = "localtimestamp"
var_label6 = "ups-temperature"
var_label7 = "load"
var_label8 = "ambient-temp"
var_label9 = "rh"

def get_values (device_label, var_label, items):
    base_url = "http://things.ubidots.com/api/v1.6/devices/" + device_label + "/" + var_label + "/values"
    try:
        req = requests.get(base_url + '?token=' + token + "&page_size=" + str(items), timeout=20)
        jsondata = req.json()
        return jsondata
    except:
        print ("error: Request failed or timed out")
        return False

if __name__ == '__main__': 
    #create connection with database
    connection = mysql.connector.connect(host='localhost',
                                database='databasename',
                                user='username',
                                password='password')
    k = 0
    while k < 2:
        #determine the amount of data rows requested
        x=100
        #get data from ubidots
        values1 = get_values(device_label, var_label1, x)
        values2 = get_values(device_label, var_label2, x)
        values3 = get_values(device_label, var_label3, x)
        values4 = get_values(device_label, var_label4, x)
        values5 = get_values(device_label, var_label5, x)
        values6 = get_values(device_label, var_label6, x)
        values7 = get_values(device_label, var_label7, x)
        values8 = get_values(device_label, var_label8, x)
        values9 = get_values(device_label, var_label9, x)
        #check the completeness of each data
        len1 = len (values1 ['results'])
        len2 = len (values2 ['results'])
        len3 = len (values3 ['results'])
        len4 = len (values4 ['results'])
        len5 = len (values5 ['results'])
        len6 = len (values6 ['results'])
        len7 = len (values7 ['results'])
        len8 = len (values8 ['results'])
        len9 = len (values9 ['results'])
        a = [len1,len2,len3,len4,len5,len6,len8]
        print (a)       
        #print (len1)
        if len1 and len2 and len3 and len4 and len5 and len6 and len7 and len8 and len9 > 0:
            #execute parsing data
            print ('Execute program')
            #access row data
            n = 0
            for i in range (0,len(values6 ['results'])):
                #timestamp
                time1 = values5['results']
                times1 = time1 [n]
                ts1 = times1 ['value']
                dts = datetime.utcfromtimestamp(int(ts1)).strftime('%Y-%m-%d %H:%M:%S')
                print (dts)
                #battery charge value
                val1 = values1['results']
                vals1 = val1 [n]
                batt = vals1 ['value']
                print (batt)
                #ups status
                ctx = vals1 ['context']
                stat = ctx ['status']
                print (stat)                
                #input frequency value
                val2 = values2['results']
                vals2 = val2 [n]
                infr = vals2 ['value']
                print (infr)
                #input voltage value
                val3 = values3['results']
                vals3 = val3 [n]
                invo = vals3 ['value']
                print (invo)                
                #output voltage value
                val4 = values4['results']
                vals4 = val4 [n]
                outvo = vals4 ['value']
                print (outvo)                
                #ups temperature value
                val6 = values6['results']
                vals6 = val6 [n]
                utemp = vals6 ['value']
                print (utemp)                
                #ups load value
                val7 = values7['results']
                vals7 = val7 [n]
                uload = vals7 ['value']
                print (uload)               
                #ambient temperature value
                val8 = values8['results']
                vals8 = val8 [n]
                amtemp = vals8 ['value']
                print (amtemp)
                #relative humidity value
                val9 = values9['results']
                vals9 = val9 [n]
                rh = vals9 ['value']
                print (rh)
                n+=1
                try:
                    #check timestamp data in database
                    get_timestamp = "SELECT timestamp FROM upsmonitoring_ups5 ORDER BY timestamp DESC LIMIT 0,100"
                    cursor = connection .cursor()
                    cursor.execute(get_timestamp)
                    records = cursor.fetchall()
                    tslist = []
                    j=0
                    for i in records:
                        recordss = records [j]
                        ts = recordss [0]
                        tss = ts.strftime('%Y-%m-%d %H:%M:%S')
                        j+=1
                        tslist.append(tss)
                    if dts in tslist:
                        #don't insert data if data is exist
                        print('Record already exist')
                        continue
                    else:
                        #execute to insert data if data didn't exist
                        records_to_insert = [ (dts, batt, infr, invo, outvo, utemp, uload, amtemp, rh, stat)]
                        #insert query
                        sql_insert_query = """ INSERT INTO `upsmonitoring_upstest`
                                (`timestamp`, `batt`, `freqin`, `voltin`,`voltout`,`temp`,`load`, `ambtemp`, `rh`, `status`) 
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                        cursor = connection.cursor()
                        result  = cursor.executemany(sql_insert_query, records_to_insert)
                        connection.commit()
                        print (cursor.rowcount, "Record inserted successfully into python_users table")
                #create error message
                except mysql.connector.Error as error :
                    connection.rollback() #rollback if any exception occured
                    print("Failed inserting record into python_users table {}".format(error)) 
                finally:
                    print("Finish")
        else:
            #skip the execution if data are incomplete
            print ('Data incomplete')
            continue
        k+=1
        time.sleep(15)
