import paho.mqtt.client as mqtt
from Constants import *
from Initialization import *
from time import strftime, localtime
import json, time, os, inspect, MyModbusRTU, traceback, gc, threading, pprint, signal, psutil
import paho.mqtt.client as mqtt
import MyDB

FolderPath = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

_FINISH = False

def get_process_memory():
    process = psutil.Process(os.getpid())
    return [process.memory_info().rss,process.memory_full_info().rss]

#print(LoggingPeriod)
db = MyDB.DataBase()

def PollerPerPort(CommPort, DIDList):
    mqtt_client = [None]*len(DIDList)
    isConnected = [False]*len(DIDList)

    InitialState = True
    #isAlreadyLogged = False
    tLog0 = [0]*len(DIDList)
    
    for did in DIDList:
        profile = Dev_ID[did-1]
        Token = profile['AccessToken']
        mqtt_client[did-1] = mqtt.Client()
        mqtt_client[did-1].username_pw_set(Token)
        Trial = 1
        while(Trial <=3):
            try:
                mqtt_client[did-1].connect(BrokerHOST, MQTTPort, 60)
                mqtt_client[did-1].loop_start()
                isConnected[did-1] = True
                print("Client-%d connected to MQTT Broker" %did)
                break
            except:
                Trial += 1
                print("Client-%d FAILED connected to MQTT Broker" %did)
                time.sleep(3)

            
    while(not _FINISH):
        try:
            for did in DIDList:
                tPoll0 = time.time()    #begin polling time
                if InitialState:
                    tLog0[did-1] = tPoll0   
                profile = Dev_ID[did-1]
                deviceAddress = profile['Address']
                baudrate = profile['Baudrate']
                parity = profile['Parity']
                stopbit = profile['StopBit']
                bytesize = profile['ByteSize']
                timeout = profile['Timeout']
                byteorder = profile['ByteOrder']

                if not isConnected[did-1]:
                    try:
                        mqtt_client[did-1].connect(BrokerHOST, MQTTPort, 60)
                        mqtt_client[did-1].loop_start()
                        isConnected[did-1] = True
                        print("Client-%d connected to MQTT Broker" %did)
                        break
                    except:
                        Trial += 1
                        print("Client-%d FAILED connected to MQTT Broker" %did)
                
                try:
                    device = MyModbusRTU.Device(CommPort, deviceAddress, baudrate, parity, stopbit, bytesize, byteorder, timeout)
                    print("Connected to ", Dev_ID[did-1]['name'])
                except:
                    tb = traceback.format_exc()
                    #print(tb)
                    print("Port is closed. Please check if your modbus device is connected to the USB port")
                
                Data = {}
                
                ### Read Discrete Input Registers
                if len(DiscInVarName[did-1]) != 0:
                    DiscInData = device.read_bits(DiscInVarName[did-1],DiscInAddress[did-1],functioncode=2)
                    print(DiscInData)
                    Data.update(DiscInData)
                
                ### Read Discrete Output Registers
                if len(DiscOutVarName[did-1]) != 0:
                    DiscOutData = device.read_bits(DiscOutVarName[did-1],DiscOutAddress[did-1],functioncode=1)
                    print(DiscOutData)
                    Data.update(DiscOutData)
                    
                ### Read Input Registers
                for _DataType in DataTypes:
                    if _DataType in [INT16, INT32, INT64]:
                        command = """
if len(InRegVarName_%s[did-1]) != 0:
    InRegData = device.read_%s(InRegVarName_%s[did-1], InRegAddress_%s[did-1], InRegMultiplier_%s[did-1], signed=True, functioncode=4)
    #print(InRegData)
    Data.update(InRegData)
"""%(_DataType,_DataType,_DataType,_DataType,_DataType)
                        exec(command)
                    elif _DataType in [UINT16, UINT32, UINT64]:
                        command = """
if len(InRegVarName_%s[did-1]) != 0:
    InRegData = device.read_%s(InRegVarName_%s[did-1], InRegAddress_%s[did-1], InRegMultiplier_%s[did-1], signed=False, functioncode=4)
    #print(InRegData)
    Data.update(InRegData)
"""%(_DataType,_DataType[1:],_DataType,_DataType,_DataType)
                        exec(command)
                    elif _DataType in [FLOAT16, FLOAT32, FLOAT64]:
                        command = """
if len(InRegVarName_%s[did-1]) != 0:
    InRegData = device.read_%s(InRegVarName_%s[did-1], InRegAddress_%s[did-1], InRegMultiplier_%s[did-1], functioncode=4)
    #print(InRegData)
    Data.update(InRegData)
"""%(_DataType,_DataType,_DataType,_DataType,_DataType)
                        exec(command)
                    elif _DataType == STRING:
                        command = """
if len(InRegVarName_%s[did-1]) != 0:
    InRegData = device.read_%s(InRegVarName_%s[did-1], InRegAddress_%s[did-1], functioncode=4)
    #print(InRegData)
    Data.update(InRegData)
"""%(_DataType,_DataType,_DataType,_DataType)
                        exec(command)

                #Read Holding Registers
                for _DataType in DataTypes:
                    if _DataType in [INT16, INT32, INT64]:
                        command = """
if len(HoldRegVarName_%s[did-1]) != 0:
    HoldRegData = device.read_%s(HoldRegVarName_%s[did-1], HoldRegAddress_%s[did-1], HoldRegMultiplier_%s[did-1], signed=True, functioncode=3)
    #print(HoldRegData)
    Data.update(HoldRegData)
"""%(_DataType,_DataType,_DataType,_DataType,_DataType)
                        exec(command)
                    elif _DataType in [UINT16, UINT32, UINT64]:
                        command = """
if len(HoldRegVarName_%s[did-1]) != 0:
    HoldRegData = device.read_%s(HoldRegVarName_%s[did-1], HoldRegAddress_%s[did-1], HoldRegMultiplier_%s[did-1], signed=False, functioncode=3)
    #print(HoldRegData)
    Data.update(HoldRegData)
"""%(_DataType,_DataType[1:],_DataType,_DataType,_DataType)
                        exec(command)
                    elif _DataType in [FLOAT16, FLOAT32, FLOAT64]:
                        command = """
if len(HoldRegVarName_%s[did-1]) != 0:
    HoldRegData = device.read_%s(HoldRegVarName_%s[did-1], HoldRegAddress_%s[did-1], HoldRegMultiplier_%s[did-1], functioncode=3)
    #print(HoldRegData)
    Data.update(HoldRegData)
"""%(_DataType,_DataType,_DataType,_DataType,_DataType)
                        exec(command)
                    elif _DataType == STRING:
                        command = """
if len(HoldRegVarName_%s[did-1]) != 0:
    HoldRegData = device.read_%s(HoldRegVarName_%s[did-1], HoldRegAddress_%s[did-1], functioncode=3)
    #print(HoldRegData)
    Data.update(HoldRegData)
"""%(_DataType,_DataType,_DataType,_DataType)
                        exec(command)
                
                tPoll1 = time.time()    #end polling time
                
                #PollingDuration = round(tPoll1-tPoll0, 2)
                PollingDuration = tPoll1-tPoll0
                Data['PollingDuration'] = PollingDuration
                Timestamp = strftime("%Y-%m-%d %H:%M:%S", localtime())
                Data['Timestamp'] = Timestamp

                #pprint.pprint(Data, width=1)
                
                with open(FolderPath + '/JSON/Data/Device%dData.json'%did, 'w') as file:
                    file.write(json.dumps(Data, indent=4))

                tLog1 = tPoll1
                if InitialState or tLog1 - tLog0[did-1] >= LoggingPeriod:
                    db.InsertData(did, Dev_ID[did-1]['name'], Data)
                    db.commit()
                    InitialState = False
                    #isAlreadyLogged = True
                    print(Timestamp, ">>>", Dev_ID[did-1]['name'], "Data are Logged")
                    tLog0[did-1] = tPoll1
                    
                DataPerTopic = []
                for i in range(0,len(VarsPerTopic)):
                    vals = []
                    for name in VarsPerTopic[i][did-1]:
                        vals.append(Data[name])
                    DataPerTopic.append(dict(zip(VarsPerTopic[i][did-1],vals)))
                #pprint.pprint(DataPerTopic, width=1)

                i = 0
                for topic in TopicList:
                    if DataPerTopic[i] != {}:
                        mqtt_client[did-1].publish(topic, json.dumps(DataPerTopic[i]),0)
                        print(Timestamp, ">>>", Dev_ID[did-1]['name'], "Data are Published in topic:", topic)
                    i += 1
                
            time.sleep(PollingInterval)
                
        except KeyboardInterrupt:
            for did in DIDList:
                mqtt_client[did-1].loop_stop()
            print('Interrupted')
            break
        except:
            tb = traceback.format_exc()
            print(tb)
            time.sleep(PollingInterval)

class ServiceExit(Exception):
    pass

def service_shutdown(signum, frame):
    print('Caught signal %d' % signum)
    raise ServiceExit

if __name__ == '__main__':
    # Register the signal handlers
    signal.signal(signal.SIGTERM, service_shutdown)
    signal.signal(signal.SIGINT, service_shutdown)
    try:
        CommPorts = list(DevPerPortList.keys())
        DID = list(DevPerPortList.values())
        
        threads = []
        i = 0
        for _port in CommPorts:
            threads.append(threading.Thread(target=PollerPerPort, args=[_port,DID[i]]))
            threads[i].setDaemon(True)
            threads[i].start()

        while(True):
            time.sleep(0.5)
            
    except ServiceExit:
        print("finished")
        _FINISH = True
        i = 0
        for _port in CommPorts:
            threads[i].join()
        print('All Thread Stopped')
        db.close()  
    except:
        tb = traceback.format_exc()
        #print(tb)
        db.close()
        
#PollerPerPort('COM9', [1])
