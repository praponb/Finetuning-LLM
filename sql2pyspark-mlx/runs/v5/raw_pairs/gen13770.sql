with ranked_products as (
    select *, dense_rank() over (partition by supplier_id order by cost desc) as pos
    from products
    where supplier_id = 1
)
select id, supplier_id, cost
from ranked_products
where pos <= 2
