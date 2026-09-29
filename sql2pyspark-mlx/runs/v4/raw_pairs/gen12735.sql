select
    id,
    customer_id,
    created_at,
    items,
    attributes,
    size(items) as num_items,
    forall(items, i -> i.qty <= 5) as all_small_qty
from carts
where created_at < '2024-02-01'
