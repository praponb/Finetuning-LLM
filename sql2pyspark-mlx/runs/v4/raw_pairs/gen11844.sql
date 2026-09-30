select
    id,
    product_id,
    quantity,
    dense_rank() over (partition by product_id order by quantity desc) as dense_rnk
from inventory
where product_id in (5, 6, 8)
