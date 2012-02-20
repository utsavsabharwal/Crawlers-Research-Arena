f=open("success.log").readlines()
f1=open("upload.log","a+")
for x in f:
     x=x.replace("-->","")
     x=x.replace(".uss","")
     f1.write(x.split(":::")[0].strip()+chr(10))

f1.flush()


