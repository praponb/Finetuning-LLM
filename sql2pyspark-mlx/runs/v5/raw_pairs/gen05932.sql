select id, order_id
from order_items
where quantity < 7 or product_id is null;
