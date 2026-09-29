select
    order_id,
    round(stddev(quantity), 2) stddev_quantity,
    avg(unit_price) avg_unit_price,
    min(quantity) min_quantity
from order_items
where quantity < 7 and quantity = 7 and product_id = 38
group by order_id
having round(stddev(quantity), 2) > 41
order by avg_unit_price desc;
