f=open("urls").readlines()
domains = open("domains").readlines()
for x in f:
    temp = x.split("\t")
    try:
	    for y in domains:
		if(x.find(y.strip())>-1):
	    		filename = "input/"+ temp[0]
	    		f1 = open(filename, "a+")
	    		f1.write((x))
	    		f1.close()
			break
    except Exception, ex:
	    print ex


