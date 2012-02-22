phase = str(phase)
import gzip
from cStringIO import StringIO
import boto.exception
import boto.s3
import boto.s3.connection
import boto.s3.key
__all__ = ['Starter']

import httplib
import smtplib
from multiprocessing.managers import BaseManager
import socket
import time
import urllib2
import sys
from veetwo.lib.aggregation.spider import get_spider_config_value as c
from veetwo.lib.aggregation.spider.doc_store import DocStoreS3
from veetwo.lib.aggregation.spider.model import Document
from veetwo.lib.aggregation.spider.urlretriever import URLRetriever, URLRetrieveError
from veetwo.lib.aggregation.spider.docprocessor_queue import DocProcessorQueue

#from veetwo.tools.jobs.command import inject_logging
import logging
L = logging
D = logging.debug
DOCUMENT_STORE_BUCKET_NAME = c('documentstore.bucketname')

class GzipWrapper(object):

    def __init__(self, *args, **kwargs):
        self.gzfile = gzip.GzipFile(*args, **kwargs)

    def __enter__(self):
        return self.gzfile

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.gzfile:
            self.gzfile.close()

class S3(object):
            
    def __init__(self):
        self.s3conn = boto.s3.connection.S3Connection()
        assert self.s3conn, 'Could not connect to S3'
        self.bucket = self.s3conn.lookup('com.wize.spider.sandbox.docstore')
        if not self.bucket:
            self.bucket = self.s3conn.create_bucket('com.wize.spider.docstore')
            assert self.bucket, 'Could not create bucket %r' % bucket_name
        assert self.bucket, 'Cannot lookup bucket %r' % bucket_name
	
        
        
    def upload(self, docid, page_text):
        try:
            key = boto.s3.key.Key(self.bucket, name=docid)   
            headers = {'Content-Type': 'text/html',
                       'Content-Encoding': 'gzip'}
            key.set_contents_from_string(self._gzip(page_text), headers=headers)     
            return True
        except Exception, ex:
            print "Failed to upload to SQS", ex
            raise
        
    def _gzip(self, text):
        try:
            ff = StringIO()
            with GzipWrapper(mode='wb', fileobj=ff) as gz:
                gz.write(text)
            return ff.getvalue()
        except Exception, ex:
            print "Failed to compress the file", ex
            raise 


print "Reading UIDs"
docids=open("/mnt/curl/"+phase+"/output/upload.log").readlines()
print "Connecting to Amazon SQS"
s=S3()
#print "Uploading to SQS"
while docids:
    docid=str(docids.pop().strip())
    print docid
    try:
        page_text=open("/mnt/curl/"+phase+"/"+docid+".uss").read()
	#print len(page_text)
	#rint page_text
    except Exception, ex:
        print "File not found", docid, ex
        continue
    try:        
        result = s.upload(docid, page_text)
        assert result, "Failed to upload to S3"
        #print "Uploaded to S3"
    except Exception, ex:
        print "Failed to upload to S3", ex
        continue 
    try:
        DocProcessorQueue(name="docprocessor_queue_beta_spider", logger=L).put(docid, docid)
	#print "Uploaded to SQS"
    except Exception, ex:
        print "Failed to upload to SQS", ex
    

    
