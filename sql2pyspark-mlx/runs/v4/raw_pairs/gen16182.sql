select order_id, max(unit_price) as max_unit_price, round(stddev(quantity), 2) as stddev_quantity
from order_items
where product_id is null
group by order_id
order by stddev_quantity desc, max_unit_price;
