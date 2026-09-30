select product_id, unit_price, coalesce(quantity, 0) as quantity_filled
from order_items
where product_id in (18, 30, 32);
