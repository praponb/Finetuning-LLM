select
    id,
    created_at,
    coupon_codes,
    items,
    attributes,
    aggregate(items, 0D, (acc, x) -> acc + x.qty * x.price, acc -> round(acc, 2)) as cart_value,
    attributes['utm_source'] as utm_source
from carts;
