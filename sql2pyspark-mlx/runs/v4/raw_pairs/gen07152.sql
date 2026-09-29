select product_id, avg_quantity, first_last_restock
from (
    select product_id, avg(quantity) avg_quantity, min(last_restock) first_last_restock
    from inventory
    group by product_id
) x
where avg_quantity > 110
order by product_id desc;
