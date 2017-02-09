#Copyright (c) 2017 LEI JIN
#version 0.7
#This file handle event operation as a workflow with Toil
from toil.job import Job
from toil.common import Toil
import time
import random


# def enterance(job,message,memory="2G", cores=4, disk="3G"):# the enterance of the eventHandler 
# 	_event_occur=event_occur()
# 	if _event_occur
message={"eventOccur":True,
		"notification":"The event event occurs,this is the notification",
		"records":"this is the records",
		"eventType":"EXCEP",# 'INFO' 'EXCEP' 'WARN'
		"exceptType":'ACCIDENT',#'ACCIDENT' 'ISSUE' 'CHANGE'
		"furtherOperation":True,#Boolean
		"replyMethod":"ALARM",#"AUTO" "ALARM" "EXCEP"
		"needAction":True,#Boolean
		"isSuccessful":True #Boolean
}
		
def event_occur(job,message,memory="2G", cores=4, disk="3G"):
	job.fileStore.logToMaster("in event_occur",level=100)
	flag=message["eventOccur"]
	return flag #boolean

def event_notification(job,message,memory="2G", cores=4, disk="3G"):
	"""notify"""
	job.fileStore.logToMaster("in event_notification",level=100)
	content=message["notification"]
	job.fileStore.logToMaster(content,level=100)

 
def event_detect(job,message,memory="2G", cores=4, disk="3G"):#detect events
    """detect the events"""
    job.fileStore.logToMaster("in event_detect",level=100)

def event_record_persistent(job,message,memory="2G", cores=4, disk="3G"):#keep log all the time,file i/o
    """local message storage """
    job.fileStore.logToMaster("in record_persistent",level=100)
    record=message["records"]
    time_stamp=time.localtime(time.time())
    strs=(str(time_stamp.tm_year)+str(time_stamp.tm_mon)+str(time_stamp.tm_mday)+str(time_stamp.tm_hour)+str(time_stamp.tm_min)+str(time_stamp.tm_sec)+str(random.randint(1000,9999)))	
    """the naming format is year+mon+day+h+m+s+random(1000~9999)"""
    job.fileStore.logToMaster(record+strs,level=100)
    url="/Users/leijin/Documents/CCSC-W/"
    with open(url+strs+".txt",'w') as fo:
		fo.write(record)
# def event_record_persistent(job,message,memory="2G", cores=4, disk="3G"):#keep log all the time,file i/o
#     """local message storage """
#     job.fileStore.logToMaster("in record_persistent",level=100)
#     record=message["records"]
#     time_stamp=time.localtime(time.time())
#     strs=(str(time_stamp.tm_year)+str(time_stamp.tm_mon)+str(time_stamp.tm_mday)+str(time_stamp.tm_hour)+str(time_stamp.tm_min)+str(time_stamp.tm_sec)+str(random.randint(1000,9999)))	
#     """the naming format is year+mon+day+h+m+s+random(1000~9999)"""
#     job.fileStore.logToMaster(record+strs,level=100)
#     scratchFile = job.fileStore.getLocalTempFile()
#     with open(scratchFile, 'w') as fH: # Write something in the # scratch file.
#         fH.write(record)
#     fileID = job.fileStore.writeGlobalFile(scratchFile)
#     # job.fileStore.logToMaster(fileID,level=100)
#     exportFile(fileID, strs)
  
#   #   with open(strs+".txt",'w') as fo:
# 		# fo.write(record)


def event_filter_1(job,message,memory="2G", cores=4, disk="3G"):
	"""select the type of the event"""
	job.fileStore.logToMaster("in event_filter_1",level=100)
	event_type=message["eventType"]
	if event_type=='INFO':
		job.addChildJobFn(event_end)
	elif event_type=='EXCEP':
		job.addChildJobFn(event_excep_select,message)
	elif event_type=='WARN':
		job.addChildJobFn(event_filter_2,message)
	else:
		job.fileStore.logToMaster('NO SUCH EVENT TYPE',level=100)
		job.addChildJobFn(event_end)

def event_filter_2(job,message,memory="2G", cores=4, disk="3G"):
	"""decide whether further operation is needed for the second selection"""
	job.fileStore.logToMaster("event_filter_2",level=100)
	tag=message["furtherOperation"]
	if tag==False:
		job.addChildJobFn(event_end)
	else:
		job.addChildJobFn(event_choose_reply,message)


def event_excep_select(job,message,smemory="2G", cores=4, disk="3G"):
	"""select excption type"""
	job.fileStore.logToMaster("event_excep_select",level=100)
	excep_type=message["exceptType"]
	if excep_type=='ACCIDENT':
		job.addChildJobFn(event_accident_management,message)
	elif excep_type=='ISSUE':
		job.addChildJobFn(event_issue_management,message)
	elif excep_type=='CHANGE':
		job.addChildJobFn(event_change_management,message)
	else:
		job.fileStore.logToMaster("NO SUCH EXCEPTION TYPE",level=100)
		job.addChildJobFn(event_end)


def event_choose_reply(job,message,memory="2G", cores=4, disk="3G"):
	"""choose the way to reply to the event"""
	job.fileStore.logToMaster("in event_choose_reply",level=100)
	reply=message["replyMethod"]
	if reply=="AUTO":
		job.addChildJobFn(event_automatic_reply,message)
	elif reply=="ALARM":
		job.addChildJobFn(event_alarm,message)
	elif reply=="EXCEP":
		job.addChildJobFn(event_excep_select,message)
	else:
		job.fileStore.logToMaster("NO SUCH EXCEPTION TYPE",level=100)
		job.addChildJobFn(event_end)

def event_automatic_reply(job,message,memory="2G", cores=4, disk="3G"):
	"""automatically reply """
	job.fileStore.logToMaster("in event_automatic_reply",level=100)
	job.fileStore.logToMaster("AUTO REPLY",level=100)
	job.addChildJobFn(event_need_action,message)

def event_alarm(job,message,memory="2G", cores=4, disk="3G"):
	"""alarm"""
	job.fileStore.logToMaster("in event_alarm",level=100)
	job.fileStore.logToMaster("ALARM",level=100)
	job.addChildJobFn(event_human_work,message)

def event_human_work(job,message,memory="2G", cores=4, disk="3G"):
	"""Human work"""
	job.fileStore.logToMaster("in event_human_work",level=100)
	job.fileStore.logToMaster("HUMAN OPERATION START",level=100)
	job.addChildJobFn(event_need_action,message)

def event_accident_management(job,message,memory="2G", cores=4, disk="3G"):
	"""accident management"""
	job.fileStore.logToMaster("in event_accident_management",level=100)
	job.addChildJobFn(event_need_action,message)

def event_issue_management(job,message,memory="2G", cores=4, disk="3G"):
	"""issue management"""
	job.fileStore.logToMaster("in event_issue_management",level=100)
	job.addChildJobFn(event_need_action,message)

def event_change_management(job,message,memory="2G", cores=4, disk="3G"):
	"""change management"""
	job.fileStore.logToMaster("in event_change_management",level=100)
	job.addChildJobFn(event_need_action,message)



def event_need_action(job,message,memory="2G", cores=4, disk="3G"):
	"""the second judgement"""
	job.fileStore.logToMaster("in event_need_action",level=100)
	flag=message["needAction"]
	if flag==True:
		job.addChildJobFn(event_implement,message)
	else:
		job.addChildJobFn(event_view_result,message)


def event_implement(job,message,memory="2G", cores=4, disk="3G"):
	"""implement all the requirements"""
	job.fileStore.logToMaster("in event_implement",level=100)
	job.addChildJobFn(event_view_result,message)

def event_view_result(job,message,memory="2G", cores=4, disk="3G"):
	"""check the result after operation above"""
	job.fileStore.logToMaster("in event_view_result",level=100)
	is_successful=message["isSuccessful"]
	if is_successful==True:
		job.addChildJobFn(event_end)
	else:
		job.addChildJobFn(event_excep_select,message)

def event_end(job,memory="2G", cores=4, disk="3G"):
	"the end of the event"
	job.fileStore.logToMaster("EVENT END",level=100)
	


job_event_occur=Job.wrapJobFn(event_occur, message)
job_event_notification=job_event_occur.addChildJobFn(event_notification,message)
job_event_detect=job_event_notification.addChildJobFn(event_detect,message)
job_event_record=job_event_detect.addChildJobFn(event_record_persistent,message)
job_event_filter1=job_event_record.addChildJobFn(event_filter_1,message,job_event_record)

if __name__=="__main__":
	options = Job.Runner.getDefaultOptions("./toilWorkflowRun") 
	options.logLevel = "INFO"
	Job.Runner.startToil(job_event_occur, options)









