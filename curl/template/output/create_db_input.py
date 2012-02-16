import smtplib
contents=open("success.log").readlines()
success = open("update","a+")
failure = open("disable", "a+")
insert = open("insert", "a+")
success_count=0
failure_count=0
disable_count=0
total_count=0
total_count+=len(contents)
def send_mail(subject, body):
    sender = "db_update@nextag.com"
    receiver = 'utsav.sabharwal@nextag.com', 'jayant.yadav@nextag.com'
    msg = ("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s" 
        %(sender, receiver, subject, body))
    s = smtplib.SMTP('localhost')
    s.sendmail(sender, ['utsav.sabharwal@nextag.com', 'jayant.yadav@nextag.com'], msg)
    s.quit()
    
for content in contents:
    
    content = content.replace("-->","")
    filename, original_url, final_url = content.split(":::")
    original_url = original_url.strip()
    final_url = final_url.strip()
    if(original_url==final_url):
        try:
            success.write(original_url+chr(10))
            success_count+=1
        except Exception, ex:
            msg = str(original_url+ex)
            send_mail("Error writing url from success.log to update", msg)
        
    else:
        try:
            failure.write(original_url+chr(10))
        except Exception, ex:
            msg = str(original_url+ex)
            send_mail("Error writing url from success.log to disable", msg)        
        try:
            msg = original_url+"\t"+final_url+chr(10)
            insert.write(msg)
        except Exception, ex:
            msg = str(original_url+ex)
            send_mail("Error writing url from success.log to insert", msg)        
        failure_count+=1
        disable_count+=1

contents=open("failure.log").readlines()    
total_count+=len(contents)
for content in contents:
    content = content.replace("-->","")
    filename, url, error_code, error = content.split(":::")
    url = url.strip()
    try:
        failure.write(url+chr(10))
        failure_count+=1
    except Exception, ex:
        msg = str(original_url+ex)
        send_mail("Error writing url from failure.log to insert", msg)        
        
msg="Total URLs Processed: "+str(total_count)+"\nTotal URLs to be Updated: "+str(success_count)+"\nTotal URLs to be Disabled: "+str(failure_count)+"\nTotal URLs to be Inserted: "+str(disable_count)
send_mail("DB Operation Scheduling Stat", msg)
print "Done -:)"