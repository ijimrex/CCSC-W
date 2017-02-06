from toil.job import Job
from toil.common import Toil
import time
import random


def enterance(job,message,memory="2G", cores=4, disk="3G"):# the enterance of the eventHandler 
	_event_occur=event_occur()
	if _event_occur

		
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

class event_record_persistent(Job):#keep log all the time,file i/o
    """local message storage """
    #need messages
	def __init__(self,message):
		self.record=message
	def store(self):
		time_stamp=time.localtime(time.time())
		strs=(str(time_stamp.tm_year)+str(time_stamp.tm_mon)+str(time_stamp.tm_mday)+str(time_stamp.tm_hour)+str(time_stamp.tm_min)+str(time_stamp.tm_sec)+str(random.randint(1000,9999)))	
		"""the naming format is year+mon+day+h+m+s+random(1000~9999)"""
		with open(strs+".txt",'w') as fo:
			fo.write(self.record)
	def show_record(self):
		print self.record


def event_filter(job,message,memory="2G", cores=4, disk="3G"):
	event_type=message
	if event_typetype=='INFO':		
	elif event_typetype=='EXCEP':
	elif event_typetype=='WARN':
	else:
		print 'NO SUCH EVENT TYPE'






