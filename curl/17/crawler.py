import md5
import sys
import time

def crawl(urls, sbook, fbook, num_conn=500):

    success_count = 0
    failure_count = 0
    start_time = time.time()
    import sys
    import pycurl

    # We should ignore SIGPIPE when using pycurl.NOSIGNAL
    
    try:
        import signal
        from signal import SIGPIPE, SIG_IGN
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)
    except ImportError:
        pass

    # Make a queue with (url, filename) tuplev").hes
    queue = []
    for url in urls:
        url = url.strip()
        if not url or url[0] == "#":
            continue
        filename = str(md5.new(url).hexdigest())+".uss"
        queue.append((url, filename))


    # Check args
    assert queue, "no URLs given"
    num_urls = len(queue)
    num_conn = min(num_conn, num_urls)
    assert 1 <= num_conn <= 10000, "invalid number of concurrent connections"
    print "I got ", num_urls, " URLs to process.. ."
    

    # Pre-allocate a list of curl objects
    m = pycurl.CurlMulti()
    m.handles = []
    for i in range(num_conn):
        c = pycurl.Curl()
        c.fp = None
        c.setopt(pycurl.FOLLOWLOCATION, 1)
        c.setopt(pycurl.MAXREDIRS, 3)
        c.setopt(pycurl.CONNECTTIMEOUT, 60)
        c.setopt(pycurl.TIMEOUT, 300)
        c.setopt(pycurl.LOW_SPEED_LIMIT, 0)
        c.setopt(pycurl.LOW_SPEED_TIME, 0)
        c.setopt(pycurl.NOSIGNAL, 1)
        m.handles.append(c)


    # Main loop
    freelist = m.handles[:]
    num_processed = 0
    while num_processed < num_urls:

        # If there is an url to process and a free curl object, add to multi stack
        while queue and freelist:
            url, filename = queue.pop(0)
            c = freelist.pop()
            c.fp = open(filename, "wb")
            c.setopt(pycurl.URL, url)
            c.setopt(pycurl.WRITEDATA, c.fp)
            m.add_handle(c)
            # store some info
            c.filename = filename
            c.url = url
        # Run the internal curl state machine for the multi stack
        while 1:
            ret, num_handles = m.perform()
            if ret != pycurl.E_CALL_MULTI_PERFORM:
                break
        # Check for curl objects which have terminated, and add them to the freelist
        while 1:

            num_q, ok_list, err_list = m.info_read()

            for c in ok_list:
                c.fp.close()
                c.fp = None
                m.remove_handle(c)
                success_count+=1
                pattern = "-->"+str(c.filename)+":::"+str(c.url)+":::"+str(c.getinfo(pycurl.EFFECTIVE_URL))+chr(10)
                sbook.write(pattern)
                sbook.flush()
                freelist.append(c)
            for c, errno, errmsg in err_list:
                c.fp.close()
                c.fp = None
                m.remove_handle(c)
                failure_count+=1
                pattern = "-->"+str(c.filename)+":::"+str(c.url)+":::"+str(errno)+":::"+str(errmsg)+chr(10)
                fbook.write(pattern)
                fbook.flush()
                freelist.append(c)
            num_processed = num_processed + len(ok_list) + len(err_list)
            if num_q == 0:
                break
        msg = "Total Processed:"+str(num_processed)+", Ok:"+str(success_count)+", Not Ok:"+str(failure_count)+", Time:"+str(time.time()-start_time)
        #sys.stdout.write("\r"+str(msg))
        #sys.stdout.flush()
        # Currently no more I/O is pending, could do something in the meantime
        # (display a progress bar, etc.).
        # We just call select() to sleep until some more data is available.
        m.select(1.0)


    # Cleanup
    for c in m.handles:
        if c.fp is not None:
            c.fp.close()
            c.fp = None
        c.close()
    m.close()
    return num_processed, success_count, failure_count