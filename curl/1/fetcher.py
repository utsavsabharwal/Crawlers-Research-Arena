import os
import crawler
import time
pointers=[]
print "Creating pointers to inputs"
for filename in os.listdir('input'):
	f = open("input/"+filename)
	pointers.append(f)
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
			if(len(urls)>11):
				break
	print urls
	crawler.crawl(urls, sbook, fbook)
	#print "yoyo11111111111111111111111111111111111111111111111111111111111"
	if(terminator == 0):
		print "Done"
		break

