with Ranked as (
    select *, rank() over (partition by store_id order by quantity desc) as rnk
    from inventory
    where product_id = 2
)
select
    id,
    store_id,
    quantity,
    product_id
from Ranked
where rnk = 1
