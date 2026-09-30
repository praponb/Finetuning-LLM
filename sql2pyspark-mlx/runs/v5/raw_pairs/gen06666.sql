select
    id,
    customer_id,
    created_at,
    coupon_codes,
    items,
    attributes,
    coalesce(ship_to.zip, 'UNKNOWN') as zip_or_unknown,
    attributes['device'] as device,
    transform(items, x -> x.product_id) as product_ids
from carts
