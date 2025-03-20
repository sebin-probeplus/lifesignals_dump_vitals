import time
import csv
from datetime import datetime,timedelta
import subprocess
import json
import pytz
timezone = pytz.timezone('Asia/Kolkata')
utc_tz = pytz.timezone('UTC')

def check_output(command):
	process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell=True)
	out, err = process.communicate()
	return(out)

def vitalsRetriever(patchId,intervalStop,patchStartTime):
    vitals = ["HR","RR","SKINTEMP"]
    vitalsDict = dict.fromkeys(vitals)
    vitalsDict={"HR": {"val":None,"timediff":float('inf')}, "RR":{"val":None,"timediff":float('inf')},"SKINTEMP":{"val":None,"timediff":float('inf')},"SPO2": {"val":None,"timediff":float('inf')}}
    
    patchStart= patchStartTime
    patchStartTime = datetime.fromtimestamp(patchStartTime).astimezone(tz=timezone)
    fromTime = intervalStop - timedelta(seconds=250)
    toTime = intervalStop + timedelta(seconds=250)
    fromTimeTsECG = fromTime - patchStartTime
    fromTimeTsECG = fromTimeTsECG.total_seconds() * 1e6
    toTimeTsECG = toTime - patchStartTime
    toTimeTsECG = toTimeTsECG.total_seconds() * 1e6
    
    eventts = intervalStop.timestamp()

    command = '''mongoexport --host=localhost --port=27017 --collection=%s_stream --db=mylsdb  -q='{"TsECG":{"$gte":%d}, "TsECG":{"$lte":%d}}' --out events.json '''%(patchId,fromTimeTsECG,toTimeTsECG)
    print(command)
    response = check_output(command).decode()
    # print(response)
    with open("./events.json") as file:
        for line in file:
            try:
             pkt = json.loads(line)
            except:continue
            # print(data)
            for vital in vitals:
                if len(pkt[vital]) > 0 and pkt[vital][0]>0:
                    val = pkt[vital][0]
                else:
                    val = None
                
                if val is None:
                    continue

                seq = pkt["Seq"]
                tsecg = pkt["TsECG"]
                pkt_ts = patchStart + (pkt['TsECG']/1e6)
                timediff = abs(eventts-pkt_ts)
                if vitalsDict[vital]["val"] is None or timediff < vitalsDict[vital]["timediff"] :
                    if vital == "SkinTemperature":
                        vitalsDict[vital]['val'] = val/100*1.8+32
                    else:
                        vitalsDict[vital]['val'] = val
                        vitalsDict[vital]['timediff'] = timediff
                        vitalsDict[vital]['seq'] = seq
                        vitalsDict[vital]['tsecg'] = tsecg
                        vitalsDict[vital]['dt'] = datetime.fromtimestamp(pkt_ts).astimezone(tz=timezone).strftime("%d %b %Y %I:%M:%S %p %Z")
                if timediff > vitalsDict[vital]['timediff']:
                    vitals.remove(vital)

        print(vitalsDict)    
        vitalsDict = {key:vitalsDict[key]["val"] for key in vitalsDict.keys() }



    fromTime = fromTime - timedelta(seconds=750)
    toTime = toTime + timedelta(seconds=750)
    fromTimeTsECG = fromTime - patchStartTime
    fromTimeTsECG= fromTimeTsECG.total_seconds()* 1e6
    toTimeTsECG = toTime - patchStartTime
    toTimeTsECG = toTimeTsECG.total_seconds()* 1e6
    command = '''mongoexport --host=localhost --port=27017 --collection=%s_stream --db=mylsdb  -q='{"TsECG":{"$gte":%d}, "TsECG":{"$lte":%d}}' --out vitalsSPO2.json '''%(patchId,fromTimeTsECG,toTimeTsECG)
    response = check_output(command).decode()
        # print(response)
    with open("./vitalsSPO2.json") as file:
        for line in file:
            data = json.loads(line)
            if len(data["SPO2"]) > 0 and  data["SPO2"][0] > 0 and data["SPO2"][0] <= 100:
                vitalsDict["SPO2"] = str(data["SPO2"][0])+"("+datetime.fromtimestamp(data["SPO2_TIME"]).astimezone(tz=timezone).strftime("%Y-%m-%d %H:%M:%S").split(" ")[1]+")"
                break
            else:
                vitalsDict["SPO2"] = None
    # print(vitalsDict)
    return vitalsDict

def vitalsIntervalParser():
    # vitals = ["HR","RR","SPO2","SKINTEMP"]
    with open("./patientDetailsSonipat.csv","r") as file:
        reader = csv.reader(file)
        for row in reader:
            # print(row)
            print(row)
            
                        
            patchStart = int(row.pop(4))
            try: 
                patchEnd = int(row.pop(4))
            except:
                patchEnd = patchStart + 86400
                #row.pop(4)
            morningSet = 8
            eveningSet = 20
            diff = eveningSet - morningSet
            # print(diff)
            patchStartTime = patchStart
            patchStart = datetime.fromtimestamp(patchStart).astimezone(tz=timezone)
            patchEnd = datetime.fromtimestamp(patchEnd).astimezone(tz=timezone)
            if patchEnd > datetime.now().astimezone(tz=timezone):
                patchEnd = datetime.now().astimezone(tz=timezone)
             #print(patchStart)
            dt = str(patchStart)[:-6].split(" ")[1].split(":")
            print(dt)
            intervalStart = patchStart
            if int(dt[0]) < morningSet:
                intervalStop = datetime(patchStart.year,patchStart.month,patchStart.day,morningSet,0,0,0)
                intervalStop = timezone.localize(intervalStop)
            elif int(dt[0]) > eveningSet:
                # print(dt)
                intervalStop = datetime(patchStart.year,patchStart.month,patchStart.day ,morningSet,0,0,0) + timedelta(days=1)
                intervalStop = timezone.localize(intervalStop)
            else:
                intervalStop = datetime(patchStart.year,patchStart.month,patchStart.day,eveningSet,0,0,0)
                intervalStop = timezone.localize(intervalStop)
            print(intervalStop)
            while(intervalStop < patchEnd ):
                print("--->",intervalStop)

                vitalsDict = vitalsRetriever(row[3],intervalStop,patchStartTime)
                if vitalsDict["SKINTEMP"] is not None and len(str(vitalsDict["SKINTEMP"]))>0 :
                    vitalsDict["SKINTEMP"] = (vitalsDict["SKINTEMP"]/1000)*1.8 + 32
                # print(vitalsDict)
                print(row+[intervalStop]+list(vitalsDict.values()))
                with open("./vitals_mod.csv","a") as vitalsFile:
                    writer = csv.writer(vitalsFile)
                    writer.writerow(row+[intervalStop.strftime("%Y-%m-%d %H:%M:%S ")]+list(vitalsDict.values()))
                intervalStart = intervalStop
                hour = int(str(intervalStart).split(" ")[1].split(":")[0])
                if hour == morningSet:
                    intervalStop = intervalStart + timedelta(seconds = diff*3600)
                elif hour == eveningSet:
                    intervalStop = intervalStart + timedelta(seconds=(24-diff)*3600)
            
        time.sleep(0.1)

if __name__ == "__main__":
    vitalsIntervalParser()