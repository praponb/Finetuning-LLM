select id, unit_price, order_id
from order_items
order by id desc
limit 1;
