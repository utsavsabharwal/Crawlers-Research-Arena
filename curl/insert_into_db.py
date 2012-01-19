
import MySQLdb as mdb
import urlparse
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
    
    
def get_hostname_from_url(url):
    hostname = urlparse.urlparse(url).hostname
    if not hostname:
        return False
    return hostname

def get_rdomain_from_url(url):
    ## FIXME: How many labels to keep from domain name?
    hostname = get_hostname_from_url(url)
    if not hostname:
        return False
    rdomain = hostname.split('.')
    rdomain = u'.'.join(rdomain[-1::-1])
    return rdomain



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
    
        con = mdb.connect('localhost', 'root', '', 'spider');
        cur = con.cursor()
        return con, cur
        
    except Exception, ex:
        send_email("SQL Connection Error", ex)
        return False

mapper= open("product_map").readlines()
def get_product_id(url):
    for x in mapper:
        if(x.find(url)>-1):
            return x.split("\t")[1]
    else:
	return False
    

con, cursor = connect()       

success = open("success.log","a+")
failure = open("failure.log","a+")
        
if(con):
    urls=open("insert","r").readlines()
    print "not ok"        
    for url in urls:
        url = url.strip()
        original_url, final_url = url.split("\t")
        metadata = '{ "original_url": "' + original_url + '"}'
	url_hash = hashlib.sha256(url).hexdigest()
        rdomain = get_rdomain_from_url(url)
	if not rdomain:
	    send_email("Error in Db Updation: Failed to find domain name", url)
	    continue
        product_id = get_product_id(original_url)
	if not product_id:
	    send_email("Error in Db Updation: No Product ID", url)
 	    continue
	product_id = product_id.strip()
        try:
            url = mdb.escape_string(final_url) 
        except Exception, ex:
	    send_email("Error in Db Updation: Failed to canonicalize url", url)
            continue
	try:
	    metadata = mdb.escape_string(metadata) 
	except Exception, ex:
	    send_email("Error in Db Updation: Failed to canonicalize metadata", metadata)
	    continue	
        
        query = """insert into url_queue set is_disabled = 0, next_fetch = 0, last_fetch = 0,  url="%s", url_hash="%s", rdomain="%s", product_id="%s", metadata="%s" """%(url, url_hash, rdomain, product_id, metadata)
	print query
        if(execute(con, cursor, query)):
            success.write(query)
            success.write(chr(10))
            success.flush()
	    print "ok"
        else:
            failure.write(query)
            failure.write(chr(10))
            failure.flush()
	    print "not ok"
else:
	send_mail("Db Updation Error: Could not connect","")
        
        
success.close()
failure.close()
send_email("Databse Update Completed", "")


