import MySQLdb as mdb
import urlparse
import hashlib
import datetime
import smtplib
import thread
import threading
import time

def send_email(subject, body):
	sender = "db_update@nextag.com"
	receiver = 'utsav.sabharwal@nextag.com', 'jayant.yadav@nextag.com'
	msg = ("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s" 
	       %(sender, receiver, subject, body))
	s = smtplib.SMTP('localhost')
	s.sendmail(sender, ['utsav.sabharwal@nextag.com', 'jayant.yadav@nextag.com'], msg)
	s.quit()

def execute(con, query, failure):
	try:
		cursor = con.cursor()
		response = cursor.execute(query)
		con.commit()
		if(response==1):
			cursor.close ()
			con.close ()			
			return
		else:
			failure.write("Zero Response ---> "+query+chr(10))
			failure.flush()
			cursor.close ()
			con.close ()			
			return



	except mdb.ProgrammingError as error:
		cursor.close ()
		con.close ()				
		failure.write("Programming Error ---> "+query+chr(10))
		failure.flush()		
		return

	except mdb.IntegrityError as e:
		cursor.close ()
		con.close ()				
		failure.write("Integrity Error ---> "+query+chr(10))
		failure.flush()		
		return	

	except Exception, ex:
		cursor.close ()
		con.close ()				
		ex = str(ex[1]) + chr(10) + "Query:" + chr(10) + query
		failure.write("Unknown Error ---> "+query+chr(10))
		failure.flush()				
		return



def get_hostname_from_url(url):
	hostname = urlparse.urlparse(url).hostname
	if not hostname:
		return False
	return hostname

def get_rdomain_from_url(url):
	hostname = get_hostname_from_url(url)
	if not hostname:
		return False
	rdomain = hostname.split('.')
	rdomain = u'.'.join(rdomain[-1::-1])
	return rdomain




def abc(number, url, failure, que, f):
	try:
		domain = get_rdomain_from_url(url)
		if not domain:
			que.append("ok")
			failure.write("Domain not found for url ---> "+url+chr(10))
			failure.flush()
			return
		try:
			url = mdb.escape_string(url) 
		except Exception, ex:
			que.append("ok")
			failure.write("URL Cannoclization Error --->"+url+chr(10))
			failure.flush()
			return
		query = """ update url_queue set is_disabled = 1 where rdomain = '%s'  and url = "%s" """%(domain, url)
		f.write(query+";"+chr(10))
		f.flush()
		#execute(mdb.connect('10.241.31.96', 'root', '', 'spider'), query, failure)
		que.append("ok")
	except Exception, ex:
		que.append("ok")
		failure.write("Operation Failed ---> "+url+str(ex)+chr(10))
		failure.flush()
		

def start():
	count = 0
	total_count = 0
	que = []	
	failure = open("disable_failure.log","w+")
	f=open("d","a+")
	urls=open("disable","r").readlines()
	print len(urls)
	urls=list(set(urls))
	print len(urls)
	for url in urls:
		try:
			count+=1
			url = url.strip()
			try:
				thread.start_new_thread(abc,("Thread No 1", url, failure, que, f))
			except Exception, ex:
				print "Thread Error", ex
			if(count%300==0):
				while 1:
					if(len(que)==300):
						count = 0
						del que[:]
						break
		except Exception, ex:
			print "Error:", ex
	


start()
