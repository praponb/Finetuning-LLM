select product_id, quantity_range, avg_quantity
from (
    select product_id, max(quantity) - min(quantity) as quantity_range, avg(quantity) as avg_quantity
    from inventory
    group by product_id
) as agg
where avg_quantity > 70
