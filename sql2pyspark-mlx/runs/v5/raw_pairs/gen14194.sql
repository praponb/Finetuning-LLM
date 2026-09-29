select id, launch_date, coalesce(price, 0) as price_filled
from products
where stock_qty <= 252 and supplier_id in (5, 14, 25)
order by launch_date desc, id desc;
