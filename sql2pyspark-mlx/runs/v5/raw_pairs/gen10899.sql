select
    id,
    customer_id,
    items,
    attributes,
    coalesce(attributes['utm_source'], 'unknown') as utm_source_or_default,
    forall(items, item -> item.qty <= 5) as all_small_qty,
    try_cast(attributes['weight_kg'] as double) as weight_kg
from carts
