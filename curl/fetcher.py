__all__ = ['Starter']

import httplib
from multiprocessing.managers import BaseManager
import socket
import time
import urllib2

from veetwo.lib.aggregation.spider import get_spider_config_value as c
from veetwo.lib.aggregation.spider.doc_store import DocStoreS3
from veetwo.lib.aggregation.spider.model import Document
from veetwo.lib.aggregation.spider.urlretriever import URLRetriever, URLRetrieveError
from veetwo.lib.aggregation.spider.docprocessor_queue import DocProcessorQueue

#from veetwo.tools.jobs.command import inject_logging
import logging
L = logging
D = logging.debug
I = logging.info
W = logging.warning
X = logging.exception


SECONDS_BETWEEN_PINGS = 10
SECONDS_BETWEEN_REQUESTS = 5


class UrlBrokerClient(BaseManager):
    pass

UrlBrokerClient.register('get_broker')


URL_BROKER_ADDRESS = c('urlbroker.address')
URL_BROKER_PORT = int(c('urlbroker.port'))
URL_BROKER_AUTHKEY = c('urlbroker.authkey')

DOCUMENT_STORE_BUCKET_NAME = c('documentstore.bucketname')

TIME_TO_WAIT_FOR_OTHER_FETCHERS_TO_REGISTER = float(c('fetcher.register_wait_time', default=120))


class Starter(object):

    log_name = 'aggregation.spider.fetcher'

    def run(self):

        f = Fetcher()
        try:
            f.run_forever()
        except Exception, er:
            I('Exception %s', er)
            raise


class Fetcher(object):
    
    def send_email(self, subject, body):
        sender = "curl-uploader@nextag.com"
        receiver = 'utsav.sabharwal@nextag.com', 'jayant.yadav@nextag.com'
        msg = ("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s" 
            %(sender, receiver, subject, body))
        s = smtplib.SMTP('localhost')
        s.sendmail(sender, ['utsav.sabharwal@nextag.com', 'jayant.yadav@nextag.com'], msg)
        s.quit()   
        
    def run_forever(self):
        f=open("/mnt/curl/11/output/success.log").readlines()
        for x in f:
            print x.split(":::")
            try:
                url = x.split(":::")[2].strip()
            except Exception, ex:
                msg = ex[1]+chr(10)+x
                self.send_mail("Uploader Failure: Failed to extract url", msg)
            try:
                text = open("/mnt/curl/11/"+x.split(":::")[0][3:].strip()).read()
            except Exception, ex:
                msg = ex[1]+chr(10)+"url:"+url
                self.send_mail("Uploader Failure: File not found", msg)            
            
            try:
                document = Document(url, text)
            except Exception, ex:
                msg = ex[1]+chr(10)+"url:"+url+chr(10)+"text:"+text
                self.send_mail("Uploader Failure: Failed to create standard file", msg)
            try:
                DocStoreS3(bucket_name="com.wize.spider.sandbox.docstore").put(document)
            except Exception, ex:
                msg = ex[1]+chr(10)+"bucket_name: com.wize.spider.sandbox.docstore"+chr(10)+"url:"+url+chr(10)+"text"+text
                self.send_mail("Uploader Failure: Failed to upload to S3", msg)
            try:
                DocProcessorQueue(name="docprocessor_queue_beta_spider", logger=L).put(url=url, docid=document.id)            
            except Exception, ex:
                msg = ex[1]+chr(10)+"queue_name: docprocessor_queue_beta_spider"+chr(10)+"url:"+url+chr(10)+"text"+text
                self.send_mail("Uploader Failure: Failed to upload to SQS", msg)
                