select
    case when unit_price >= 340 then 'HIGH' else 'LOW' end as unit_price_tier,
    count(distinct id) as distinct_ids,
    corr(quantity, unit_price) as corr_quantity_unit_price,
    corr(unit_price, quantity) as corr_unit_price_quantity
from order_items
group by unit_price_tier
order by corr_unit_price_quantity nulls last;
