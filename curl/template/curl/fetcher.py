import time
import thread
import os
import crawler
import smtplib

def send_email(subject, body):
    sender = "curl_crawler@nextag.com"
    receiver = 'utsav.sabharwal@nextag.com', 'jayant.yadav@nextag.com'
    msg = ("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s" 
           %(sender, receiver, subject, body))
    s = smtplib.SMTP('localhost')
    s.sendmail(sender, ['utsav.sabharwal@nextag.com', 'jayant.yadav@nextag.com'], msg)
    s.quit()


def fetch( urls, sbook, fbook):
    #send_email("Crawling Started", str(len(urls)))
    now=time.time()
    num_processed, success_count, failure_count = crawler.crawl(urls, sbook, fbook)
    try:
                    msg = "Total Processed: "+str(num_processed)+chr(10)+"Success Count: "+ str(success_count) +chr(10)+"Failure Count: "+str(failure_count)+"in "+str((time.time()-now)/60)+" minutes." 
		    #send_email("Crawling Successfull", msg)
    except Exception, ex:
                    send_email("Crawler Error:", ex[1])
                    
    
        
def main():
    sbook=open("output/success.log","a+")
    fbook=open("output/failure.log","a+")    
    pointers = []
    count = 0
    for filename in os.listdir("input"):
	count+=1
        filename = filename.strip()
        f = open("input/"+filename)
        pointers.append(f.readlines())
        f.close()
    while True:
      urls=[]
      for x in range(0, len(pointers)):
	  if(len(pointers[x])>0):
	    url = pointers[x].pop().strip()
	    if len(url)>5:
		urls.append(url)
      fetch(urls, sbook, fbook)        

main()
