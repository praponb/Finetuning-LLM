select ss.id, ss.units, ss.product_id
from store_sales ss
where ss.product_id not in (select id from products where stock_qty < 178);
