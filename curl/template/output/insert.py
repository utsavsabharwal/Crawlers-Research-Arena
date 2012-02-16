import MySQLdb as mdb
import time
import thread
import urlparse
import hashlib
import datetime
import smtplib

mapper= open("product_map").readlines()

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

def get_product_id(url):
    for i in mapper: 
	if(i.startswith(url)):
	    return i.split("\t")[1]
    


def start():
    failure = open("insert_failure.log","w+")  
    i=open("i","a+")    
    urls=open("insert","r").readlines()
    urls=list(set(urls))
    l = len(urls)
    s_count = 0
    count = 0
    for url in urls:
	print " S:", s_count, " TP:", count, " T:", l
	count+=1
	url = url.strip()
	original_url, final_url = url.split("\t")
	metadata = '{ "original_url": "' + original_url + '"}'
	rdomain = get_rdomain_from_url(final_url)
	if not rdomain:
	    print "Domain Extraction Failure"
	    failure.write("Domain Extraction Failed --->"+url+chr(10))
	    failure.flush()
	    continue
	product_id = get_product_id(original_url)
	if not product_id:
	    print "Product ID Failure", original_url
	    failure.write("Product ID Extraction Failed --->"+url+chr(10))
	    failure.flush()
	    continue
	product_id = product_id.strip()
	try:
	    url_hash = hashlib.sha256(final_url).hexdigest()
	except Exception, ex:
	    print "Hash Failure", ex
	    failure.write("URL Hash Formation Error --->"+url+chr(10))
	    failure.flush()	
	    continue
	try:
	    url = mdb.escape_string(final_url) 
	except Exception, ex:
	    print "URL Cannoclization Error", ex
	    failure.write("URL Cannoclization Error --->"+url+chr(10))
	    failure.flush()
	    continue
	try:
	    metadata = mdb.escape_string(metadata) 
	except Exception, ex:
	    print "Metadata Cannoclization Error", ex
	    failure.write("Metadata Cannoclization Error --->"+url+chr(10))
	    failure.flush()
	    continue
	try:
	    query = """insert into url_queue set is_disabled = 0, next_fetch = 0, last_fetch = 0,  url="%s", url_hash="%s", rdomain="%s", product_id="%s", metadata="%s" """%(url, url_hash, rdomain, product_id, metadata)
	    i.write(query+";"+chr(10))
	    s_count+=1
	    
	except Exception, ex:
	    print "Failed to write query", ex
	    failure.write("Unknown Error during Insertion --->"+url+chr(10))
	    failure.flush() 
	    continue
    		
start()	









