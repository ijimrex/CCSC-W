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

def event_filter_1(job,message,memory="2G", cores=4, disk="3G"):
	"""select the type of the event"""
	event_type=message[eventType]
	if event_type=='INFO':
		job.addChildJobFn(event_end)
	elif event_type=='EXCEP':
		job.addChildJobFn(event_excep_select,message)
	elif event_type=='WARN':
		job.addChildJobFn(event_filter_2,message)
	else:
		job.fileStore.logToMaster('NO SUCH EVENT TYPE')
		job.addChildJobFn(event_end)


def event_excep_select(job,message,smemory="2G", cores=4, disk="3G"):
	"""select excption type"""
	excep_type=message[exceptType]
	if excep_type=='ACCIDENT':
		job.addChildJobFn(event_accident_management,message)
	elif excep_type=='ISSUE':
		job.addChildJobFn(event_issue_management,message)
	elif excep_type=='CHANGE':
		job.addChildJobFn(event_change_management,message)
	else:
		job.fileStore.logToMaster("NO SUCH EXCEPTION TYPE")
		job.addChildJobFn(event_end)


def event_filter_2(job,message,memory="2G", cores=4, disk="3G"):
	"""decide whether further operation is needed for the second selection"""
	tag=message[furtherOperation]
	if tag==False:
		job.addChildJobFn(event_end)
	else:
		job.addChildJobFn(event_choose_reply,message)


def event_accident_management(job,message,memory="2G", cores=4, disk="3G"):
	"""accident management"""
	job.addChildJobFn(event_need_action,message)

def event_issue_management(job,message,memory="2G", cores=4, disk="3G"):
	"""issue management"""
	job.addChildJobFn(event_need_action,message)

def event_change_management(job,message,memory="2G", cores=4, disk="3G"):
	"""change management"""
	job.addChildJobFn(event_need_action,message)

def event_choose_reply(job,message,memory="2G", cores=4, disk="3G"):
	"""choose the way to reply to the event"""
	reply=message[replyMethod]
	if reply=="AUTO":
		job.addChildJobFn(event_automatic_reply,message)
	elif reply=="ALARM":
		job.addChildJobFn(event_alarm,message)
	elif reply=="EXCEP":
		job.addChildJobFn(event_excep_select,message)
	else:
		job.fileStore.logToMaster("NO SUCH EXCEPTION TYPE")
		job.addChildJobFn(event_end)

def event_automatic_reply(job,message,memory="2G", cores=4, disk="3G"):
	"""automatically reply """
	job.fileStore.logToMaster("AUTO REPLY")
	job.addChildJobFn(event_need_action,message)

def event_alarm(job,message,memory="2G", cores=4, disk="3G"):
	"""alarm"""
	job.fileStore.logToMaster("ALARM")
	job.addChildJobFn(event_human_work,message)

def event_human_work(job,message,memory="2G", cores=4, disk="3G"):
	"""Human work"""
	job.fileStore.logToMaster("HUMAN OPERATION START")
	job.addChildJobFn(event_need_action,message)

def event_need_action(job,message,memory="2G", cores=4, disk="3G"):
	flag=message[needAction]
	if flag==True:
		job.addChildJobFn(event_implement,message)
	else:
		job.addChildJobFn(event_view_result,message)


def event_implement(job,message,memory="2G", cores=4, disk="3G"):
	"""implement all the requirements"""
	job.addChildJobFn(event_view_result,message)

def event_view_result(job,message,memory="2G", cores=4, disk="3G"):
	"""check the result after operation above"""
	is_successful=message[isSuccessful]
	if is_successful==True:
		job.addChildJobFn(event_end)
	else:
		job.addChildJobFn(event_excep_select,message)

def event_end(job,memory="2G", cores=4, disk="3G"):
	"the end of the event"
	job.fileStore.logToMaster("EVENT END")
	





job_event_occur=Job.wrapJobFn(event_occur, True)
job_event_notification=job_event_occur.addChildJobFn(event_notification,message)
job_event_detect=job_event_notification.addChildJobFn(event_detect,message)
job_event_record=job_event_detect.addChildJobFn(event_record_persistent,message)
job_event_filter1=job_event_record.addChildJobFn(event_filter_1,message,job_event_record)









