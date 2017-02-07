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
	message=content
	print content

 
def event_detect(job,message,memory="2G", cores=4, disk="3G"):#detect events
    """detect the events"""
    pass

def event_record_persistent(job,message,memory="2G", cores=4, disk="3G"):#keep log all the time,file i/o
    """local message storage """
    message=record
	time_stamp=time.localtime(time.time())
	strs=(str(time_stamp.tm_year)+str(time_stamp.tm_mon)+str(time_stamp.tm_mday)+str(time_stamp.tm_hour)+str(time_stamp.tm_min)+str(time_stamp.tm_sec)+str(random.randint(1000,9999)))	
	"""the naming format is year+mon+day+h+m+s+random(1000~9999)"""
	with open(strs+".txt",'w') as fo:
		fo.write(record)
	job.fileStore.logToMaster(record)

def event_select_1(job,message,parentJob,memory="2G", cores=4, disk="3G"):
	"""select the type of the event"""
	event_type=message
	if event_typetype=='INFO':
		parentJob.addChildJobFn(event_end)
	elif event_typetype=='EXCEP':
		parentJob.addChildJobFn(excep_select)
	elif event_typetype=='WARN':
		parentJob.addChildJobFn(event_select_2)
	else:
		job.fileStore.logToMaster('NO SUCH EVENT TYPE')
		parentJob.addChildJobFn(event_end)


def excep_select(job,message,smemory="2G", cores=4, disk="3G"):
	pass

def event_end(job,memory="2G", cores=4, disk="3G"):
	"the end of the event"
	job.fileStore.logToMaster("EVENT END")




job_event_occur=Job.wrapJobFn(event_occur, True)
job_event_notification=job_event_occur.addChildJobFn(event_notification,job_event_occur.rv())
job_event_detect=job_event_notification.addChildJobFn(event_detect,job_event_notification.rv())
job_event_record=job_event_detect.addChildJobFn(event_record_persistent,job_event_detect.rv())
job_event_sel


def event_filter(job,message,memory="2G", cores=4, disk="3G"):
	event_type=message
	if event_typetype=='INFO':

	elif event_typetype=='EXCEP':
	elif event_typetype=='WARN':
	else:
		print 'NO SUCH EVENT TYPE'






