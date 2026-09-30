select
    case when unit_price >= 370 then 'PREMIUM' else 'STANDARD' end as unit_price_tier,
    round(avg(unit_price), 2) as avg_unit_price,
    round(avg(unit_price), 2) as avg_unit_price_2,
    approx_percentile(unit_price, 0.5) as unit_price_p50,
    count(distinct order_id) as distinct_order_ids,
    approx_percentile(quantity, 0.5) as quantity_p50,
    bit_or(quantity) as quantity_bit_or
from order_items
group by case when unit_price >= 370 then 'PREMIUM' else 'STANDARD' end
order by unit_price_tier;
