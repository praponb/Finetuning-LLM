select sum(price) as total_price, count(*) as cnt
from products
where supplier_id = 7 and category != 'Fashion'
order by cnt desc;
