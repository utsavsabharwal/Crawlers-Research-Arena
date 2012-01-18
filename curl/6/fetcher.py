import os
import crawler
import time
import smtplib

def send_email(sender, receiver, subject, body):
    msg = ("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s" 
        %(sender, receiver, subject, body))
    s = smtplib.SMTP('localhost')
    s.sendmail(sender, [receiver], msg)
    s.quit()
    
    



        
def fetch(pointers):
        print "Creating pointer to logs"
        sbook=open("output/success.log","a+")
        fbook=open("output/failure.log","a+")
        print "Starting Crawler"
        while (True):
                terminator = 0
                urls=[]
                for ff in pointers:
                        url = ff.readline().strip()
                        if(len(url)>5):
                                terminator+=1
                                urls.append(url)
                                '''if(len(urls)>98):
                                        break'''
                print len(urls), sbook, fbook
                try:
			if(len(urls)>0):
                        	crawler.crawl(urls, sbook, fbook)
			else:
				send_email("curl_crawler@nextag.com", "utsav.sabharwal@nextag.com, jayant.yadav@nextag.com", "No Urls left", "")
				break
                except Exception, ex:
			msg = ex+str(urls)
                	send_email("curl_crawler@nextag.com", "utsav.sabharwal@nextag.com, jayant.yadav@nextag.com", "Error in Crawler", "%s")%(msg)    
                sbook.flush()
                fbook.flush()
                '''if(terminator == 0):
                        print "Done"
                        break'''


pointers=[]
print "Creating pointers to inputs"
domains = open("domains").readlines()
for filename in domains:
	filename = filename.strip()
        f = open("input/"+filename)
        pointers.append(f)
try:
	fetch(pointers)      
except Exception, ex:
	send_email("curl_crawler@nextag.com", "utsav.sabharwal@nextag.com, jayant.yadav@nextag.com", "Error in Crawler", "%s")%(ex)
	
