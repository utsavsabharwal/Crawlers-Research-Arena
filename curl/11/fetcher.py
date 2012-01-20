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
        send_email("Crawling Started Successfully", str(len(urls)))        
        try:
            if(len(urls)>0):
                crawler.crawl(urls, sbook, fbook)
            else:
                send_email("Crawling Ended Successfully", "")
                break
        except Exception, ex:
            msg = ex[1]+chr(10)+str(urls)
            send_email("Error in Crawler", msg)
        sbook.flush()
        fbook.flush()



pointers=[]
for filename in os.listdir("input"):
    filename = filename.strip()
    f = open("input/"+filename)
    pointers.append(f)
    if(len(pointers)>299):
        send_email("Crawling Cycle Started", str(len(pointers)))
        try:
            fetch(pointers)      
        except Exception, ex:
            send_email("Error in Crawler", ex[1])
        msg = str(len(pointers))+" domains were crawled in this cycle."
        send_email("Crawling Cycle Completed", msg)
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
send_email("Crawling Cycle Completed", msg)
