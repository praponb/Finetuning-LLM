select quantity_bucket, cnt
from (
    select case when quantity >= 235 then 'high' when quantity >= 96 then 'mid' else 'low' end as quantity_bucket, count(*) as cnt
    from inventory
    where last_restock < '2023-03-01'
    group by case when quantity >= 235 then 'high' when quantity >= 96 then 'mid' else 'low' end
) as t
where cnt < 5
order by quantity_bucket desc, cnt desc
