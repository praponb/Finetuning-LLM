select
    id,
    product_id,
    quantity,
    min(quantity) over (partition by product_id) as product_id_min_quantity
from order_items
where product_id is not null;
