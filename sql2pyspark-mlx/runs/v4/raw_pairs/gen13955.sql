select
    case when quantity >= 7 then 'high' when quantity >= 2 then 'mid' else 'low' end as quantity_bucket,
    case when unit_price >= 410 then 'high' when unit_price >= 110 then 'mid' else 'low' end as unit_price_bucket,
    max(quantity) - min(quantity) as quantity_range,
    max(unit_price) - min(unit_price) as unit_price_range,
    min(unit_price) as min_unit_price
from order_items
group by quantity_bucket, unit_price_bucket
having avg(unit_price) > 270
order by unit_price_range;
