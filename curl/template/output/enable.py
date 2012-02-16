import MySQLdb as mdb
import hashlib
import datetime
import smtplib

def send_email(subject, body):
    sender = "db_update@nextag.com"
    receiver = 'utsav.sabharwal@nextag.com', 'jayant.yadav@nextag.com'
    msg = ("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s" 
        %(sender, receiver, subject, body))
    s = smtplib.SMTP('localhost')
    s.sendmail(sender, ['utsav.sabharwal@nextag.com', 'jayant.yadav@nextag.com'], msg)
    s.quit()
    
def execute(con, cursor, query):
    try:
        response = cursor.execute(query)
        con.commit()
        if(response==1):
            return True
        else:
            return False
    
    except mdb.ProgrammingError as error:
        error = str(error[1]) + chr(10) + "Query:" + chr(10) + query
        send_email("SQL Programming Error", error)
        return False
        
    except mdb.IntegrityError as e:
        e = str(e[1]) + chr(10) + "Query:" + chr(10) + query
        send_email("SQL Integrity Error", e)
        return False
    
    except Exception, ex:
        ex = str(ex[1]) + chr(10) + "Query:" + chr(10) + query
        send_email("SQL Unknown Error", ex)
        return False

    
    
    
    
    
def connect(con = None):
    try:
    
        con = mdb.connect('10.241.31.96', 'root', '', 'spider');
        cur = con.cursor()
        return con, cur
        
    except Exception, ex:
        send_email("SQL Connection Error", ex)
        return False
    
con, cursor = connect()       
success = open("success.log","a+")
failure = open("failure.log","a+")
        
if(con):
    urls=open("enable","r").readlines()
    rdomains = open("urls").readlines()
    
    for url in urls:
        url = url.strip()
        for rdomain in rdomains:
            if(rdomain.find(url)>-1):
                domain = rdomain.split("\t")[0]
        if(domain==None):
            send_email("Domain Not found", "")
            continue
        url_hash = hashlib.sha256(url).hexdigest()
        last_fetch=datetime.datetime.utcnow() 
        next_fetch=datetime.datetime.utcnow() + datetime.timedelta(days=70)
        query = """update url_queue set next_fetch = '%s', last_fetch = '%s' where rdomain = '%s'  and url_hash ='%s' and url = '%s' """%(next_fetch, last_fetch, domain, url_hash, url)
        if(execute(con, cursor, query)):
            success.write(query)
            success.write(chr(10))
            success.flush()
        else:
            failure.write(query)
            failure.write(chr(10))
            failure.flush()
        
        
success.close()
failure.close()
send_email("Databse Update Completed", "")



