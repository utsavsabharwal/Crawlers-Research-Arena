print "This file need urls, success.log and failure.log as input. \n This is going to make all the necessary queries required for db operation and would save it in the file query."
import MySQLdb as mdb
import urlparse
import hashlib
import unicodedata
query=open("query","w")    
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


def disable(uid):
    assert int(uid)
    return "update url_queue set is_disabled=1 where uid=%s ;"%(uid)

def update(uid):
    assert int(uid)
    return "update url_queue set is_disabled=0 where uid=%s ;"%(uid)

def modify(uid, url):
    url = url.decode('windows-1252')
    assert int(uid)
    try:
	url_hash = hashlib.sha256(url).hexdigest()
    except Exception, ex:
	print "Hash Failure", ex
	return
    try:
	url = mdb.escape_string(url) 
    except Exception, ex:
	print "URL Cannoclization Error", ex
	return
    rdomain = get_rdomain_from_url(url)
    if not rdomain:
	    print "Domain Extraction Failure"
	    return
    try:
	query = """update ignore url_queue set url="%s", is_disabled = 0, url_hash="%s", rdomain="%s" where uid=%s ; """%(url, url_hash, rdomain, uid)    
	return query
    except Exception, ex:
	print "Failed to write query", ex, url

def insert(element):
    url = element[1]
    url = url.decode('windows-1252')
    product_id = element[2]
    rdomain = element[3]
    try:
	    url_hash = hashlib.sha256(url).hexdigest()
    except Exception, ex:
	    print "Hash Failure", ex
    try:
	    url = mdb.escape_string(url) 
    except Exception, ex:
	    print "URL Cannoclization Error", ex    
    
    try:
	query = """insert ignore into url_queue set url="%s", is_disabled = 1, next_fetch = "2012-12-12", last_fetch = now(),  url_hash="%s", rdomain="%s", product_id="%s" ; """%(url, url_hash, rdomain, product_id)
	return query
    except Exception, ex:
	print "Failed to write query", ex, url    









print "Uploading urls file in buffer"
urls = open("urls").readlines()
db=[]
uids=[]
for x in urls:
    domain, uid, url, product_id = x.split("\t")
    db.append([uid.strip(), url.strip(), product_id.strip(), domain.strip()])
    uids.append(uid)
db.sort()
uids.sort()
if(len(uids)!=len(set(uids))):
    print "UID mismatch"
print "Working of Success.log"
success=open("success.log").readlines()
for each in success:
    each = each.replace("-->","")
    each = each.strip()
    filename, original_url, final_url = each.split(":::")
    uid=filename.replace(".uss","").strip()
    original_url = original_url.strip()
    final_url = final_url.strip()
    if(original_url ==  final_url):
	try:
        	query.write(update(uid)+chr(10))
	except Exception, ex:
		print "S1: Failed to write query", ex, uid
    else:
      try:
	final_url = final_url.decode('utf-8')
        query.write(modify(uid, final_url)+chr(10))
	uids.index(uid)
	query.write(insert(db[uids.index(uid)])+chr(10))
      except Exception, ex:
        print " S2: Failed to write query", ex, uid,final_url

print "Working on failure.log"        
failure=open("failure.log").readlines()
for each in failure:
    each = each.replace("-->","")
    each = each.strip()
    filename, url, errorcode, error = each.split(":::")
    uid=filename.replace(".uss","").strip()
    try:
	    query.write(disable(uid)+chr(10))
    except Exception, ex:
	print " F: Failed to write query", ex
query.flush()
query.close()


