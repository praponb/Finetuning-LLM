select
    case when quantity >= 9 then 'LARGE' else 'SMALL' end as quantity_tier,
    product_id,
    approx_percentile(unit_price, 0.5) as unit_price_p50,
    bit_or(quantity) as quantity_bit_or,
    min(unit_price) as min_unit_price,
    round(avg(quantity), 2) as avg_quantity
from order_items
group by 1, 2
order by avg_quantity desc;
