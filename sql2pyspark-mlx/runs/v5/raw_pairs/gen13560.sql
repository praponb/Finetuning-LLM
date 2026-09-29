select
    case when quantity >= 7 then 'LARGE' else 'SMALL' end as quantity_tier,
    product_id,
    order_id,
    stddev_samp(unit_price) as unit_price_stddev,
    percentile_approx(unit_price, 0.5) as unit_price_p50,
    round(avg(unit_price), 2) as avg_unit_price
from order_items
group by cube (case when quantity >= 7 then 'LARGE' else 'SMALL' end, product_id, order_id)
having unit_price_p50 >= 0
order by quantity_tier nulls last;
