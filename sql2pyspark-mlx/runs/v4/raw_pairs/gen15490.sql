select
    order_id,
    round(avg(quantity), 2) as avg_quantity,
    stddev_samp(unit_price) as unit_price_stddev,
    count(*) as num_rows
from order_items
group by order_id
order by unit_price_stddev desc
