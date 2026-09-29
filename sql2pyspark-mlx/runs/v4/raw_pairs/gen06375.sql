select
    customer_id,
    case when amount >= 1750 then 'LARGE' else 'SMALL' end as amount_tier,
    approx_percentile(amount, array(0.25, 0.5, 0.75)) as amount_quartiles,
    stddev_samp(shipping_fee) as shipping_fee_stddev,
    round(avg(shipping_fee), 2) as avg_shipping_fee,
    grouping(customer_id) as grouping_level
from orders
group by cube (customer_id, case when amount >= 1750 then 'LARGE' else 'SMALL' end)
order by shipping_fee_stddev desc nulls last
