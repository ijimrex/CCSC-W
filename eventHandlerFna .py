from toil.job import Job
from toil.common import Toil
import time
import random


# def enterance(job,message,memory="2G", cores=4, disk="3G"):# the enterance of the eventHandler 
# 	_event_occur=event_occur()
# 	if _event_occur

		
def event_occur(job,message,memory="2G", cores=4, disk="3G"):
	flag=message
	return flag #boolean


def event_notification(job,message,memory="2G", cores=4, disk="3G"):
	"""notify"""
	content=message[notification]
	job.fileStore.logToMaster(content)

 
def event_detect(job,message,memory="2G", cores=4, disk="3G"):#detect events
    """detect the events"""
    pass

def event_record_persistent(job,message,memory="2G", cores=4, disk="3G"):#keep log all the time,file i/o
    """local message storage """
    record=message[records]
    time_stamp=time.localtime(time.time())
    strs=(str(time_stamp.tm_year)+str(time_stamp.tm_mon)+str(time_stamp.tm_mday)+str(time_stamp.tm_hour)+str(time_stamp.tm_min)+str(time_stamp.tm_sec)+str(random.randint(1000,9999)))	
    """the naming format is year+mon+day+h+m+s+random(1000~9999)"""
    with open(strs+".txt",'w') as fo:
    	fo.write(record)
    job.fileStore.logToMaster(record)

def event_select_1(job,message,parent,memory="2G", cores=4, disk="3G"):
	"""select the type of the event"""
	event_type=message[eventType]
	if event_type=='INFO':
		parent.addChildJobFn(event_end)
	elif event_type=='EXCEP':
		parent.addChildJobFn(excep_select,message)
	elif event_type=='WARN':
		parent.addChildJobFn(event_select_2,message)
	else:
		job.fileStore.logToMaster('NO SUCH EVENT TYPE')
		parent.addChildJobFn(event_end)


def excep_select(job,message,smemory="2G", cores=4, disk="3G"):
	"""select excption type"""
	excep_type=message[exceptType]
	if 

def event_select_2(job,message,memory="2G", cores=4, disk="3G"):
	"""decide whether further operation is needed"""
	tag=message[furtherOperation]
	if tag==False:

def event_end(job,memory="2G", cores=4, disk="3G"):
	"the end of the event"
	job.fileStore.logToMaster("EVENT END")




job_event_occur=Job.wrapJobFn(event_occur, True)
job_event_notification=job_event_occur.addChildJobFn(event_notification,message)
job_event_detect=job_event_notification.addChildJobFn(event_detect,message)
job_event_record=job_event_detect.addChildJobFn(event_record_persistent,message)
job_event_select1=job_event_record.addChildJobFn(event_select_1,message,job_event_record)









