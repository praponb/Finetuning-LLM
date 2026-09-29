select
    id,
    coupon_codes,
    ship_to,
    aggregate(items, cast(0 as double), (acc, item) -> acc + item.qty * item.price, acc -> round(acc, 2)) as cart_value,
    array_join(array_sort(coupon_codes), ',') as coupons_text,
    coalesce(attributes['device'], 'unknown') as device_or_default,
    array_contains(coupon_codes, 'WELCOME5') as used_welcome5,
    coalesce(attributes['utm_source'], 'unknown') as utm_source_or_default
from carts
