import os
import crawler
import time
import smtplib


def send_email(subject, body):
    sender = "curl_crawler@nextag.com"
    receiver = 'utsav.sabharwal@nextag.com', 'jayant.yadav@nextag.com'
    msg = ("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s" 
           %(sender, receiver, subject, body))
    s = smtplib.SMTP('localhost')
    s.sendmail(sender, ['utsav.sabharwal@nextag.com', 'jayant.yadav@nextag.com'], msg)
    s.quit()





def fetch(pointers):
    sbook=open("output/success.log","a+")
    fbook=open("output/failure.log","a+")
    while (True):
        terminator = 0
        urls=[]
        for ff in pointers:
            url = ff.readline().strip()
            if(len(url)>5):
                terminator+=1
                urls.append(url)
                
        try:
            if(len(urls)>0):
                num_processed, success_count, failure_count = crawler.crawl(urls, sbook, fbook)
                try:
                    msg = "Total Processed: "+str(num_processed)+chr(10)+"Success Count: "+ str(success_count) +chr(10)+"Failure Count: "+str(failure_count) 
                    #send_email("Crawling Successfull", msg)
                except Exception, ex:
                    send_email("Crawler Error:", ex[1])
                    
            else:
                break
        except Exception, ex:
            msg = ex[1]+chr(10)+str(urls)
            send_email("Error in Crawler", msg)
        sbook.flush()
        fbook.flush()


#send_email("Phase 13 Crawling Started", "")
pointers=[]
for filename in os.listdir("input"):
    filename = filename.strip()
    f = open("input/"+filename)
    pointers.append(f)
    if(len(pointers)>299):
        try:
            fetch(pointers)      
        except Exception, ex:
            send_email("Error in Crawler", ex[1])
        msg = str(len(pointers))+" domains were crawled in this cycle."
        #send_email("Crawling Cycle Completed", msg)
        for f in pointers:
            f.close()
            pointers.remove(f)
            if(f.closed==False):
                send_email("Crawling Error: Failed to close a file pointer", "")	



send_email("Last Crawling Cycle Started", "")
try:
    fetch(pointers)      
except Exception, ex:
    send_email("Error in Crawler", ex[1])
msg = str(len(pointers))+" domains were crawled in this cycle."
send_email("Crawling Completed","")
