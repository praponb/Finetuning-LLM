select
    product_id,
    round(avg(unit_price), 2) as avg_unit_price,
    count(*) as cnt,
    round(stddev(quantity), 2) as stddev_quantity
from order_items
group by product_id
having cnt > 4
order by cnt desc, 1 desc
limit 10;
