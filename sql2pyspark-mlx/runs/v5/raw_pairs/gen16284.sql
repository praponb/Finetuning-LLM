select round(stddev(unit_price), 2) as stddev_unit_price, round(avg(unit_price), 2) as avg_unit_price, round(avg(quantity), 2) as avg_quantity
from order_items
where order_id = 22
order by avg_unit_price desc;
