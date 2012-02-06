f=open("urls").readlines()
domains = open("domains").readlines()
for x in f:
    temp = x.split("\t")
    try:
	    y = domains.index(temp[0])
	    filename = "input/"+ temp[0]
	    f1 = open(filename, "a+")
	    f1.write((temp[1]))
	    f1.close()
     except Exception, ex:
	    print ex


