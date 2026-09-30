select *
from (
    select case when quantity >= 8 then 'high' when quantity >= 4 then 'mid' else 'low' end as quantity_bucket, count(unit_price) as unit_price_count
    from order_items
    group by case when quantity >= 8 then 'high' when quantity >= 4 then 'mid' else 'low' end
) as s
where unit_price_count > 5
order by unit_price_count desc, quantity_bucket;
