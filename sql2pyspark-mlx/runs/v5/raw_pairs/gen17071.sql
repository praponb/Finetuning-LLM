select quantity_bucket, store_id, avg_quantity
from (
    select case when quantity >= 186 then 'high' when quantity >= 60 then 'mid' else 'low' end as quantity_bucket, store_id, avg(quantity) as avg_quantity
    from inventory
    where quantity between 95 and 173
    group by case when quantity >= 186 then 'high' when quantity >= 60 then 'mid' else 'low' end, store_id
) as x
where avg_quantity > 170
order by 1
