mysql -uroot -h10.241.31.96 spider -e "select distinct rdomain, uid, url, product_id from url_queue where is_disabled=0 and last_fetch=0" >> "urls";
mysql -uroot -h10.241.31.96 spider -e "select distinct rdomain from url_queue where is_disabled=0 and last_fetch=0" >> "domains"
mkdir input
mkdir output
python converter.py
python fetcher.py &
print "You may close the terminal now -:)"