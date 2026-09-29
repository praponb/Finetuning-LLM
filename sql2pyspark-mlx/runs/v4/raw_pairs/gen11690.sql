select *
from (
    select product_id, min(last_restock) as min_last_restock, min(quantity) as min_quantity
    from inventory
    where quantity >= 146
    group by product_id
) as x
where min_quantity < 220
order by min_last_restock
