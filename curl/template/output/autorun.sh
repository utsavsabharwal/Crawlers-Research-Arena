python create_db_input.py 
mysql -uroot -h10.241.31.96 spider -e "select url, product_id from url_queue where is_disabled=0 and last_fetch=0" >> product_map
mysql -uroot -h10.241.31.96 spider -e "select distinct rdomain, url from url_queue where is_disabled=0 and last_fetch=0" >> "urls"
python disable.py
mysql -h10.241.31.96 spider < d &
python enable.py &
python insert.py
mysql -h10.241.31.96 spider < i
