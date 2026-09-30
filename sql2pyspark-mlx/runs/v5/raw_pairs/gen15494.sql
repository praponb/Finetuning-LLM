select
    id,
    product_id,
    quantity,
    unit_price,
    sum(unit_price) over (partition by product_id order by quantity, id rows between unbounded preceding and current row) as running_unit_price
from order_items
