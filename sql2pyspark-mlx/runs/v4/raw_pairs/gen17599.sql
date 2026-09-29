select
    id,
    customer_id,
    created_at,
    attributes,
    array_join(array_sort(coupon_codes), ',') as coupons_text,
    attributes['device'] as device,
    attributes['gift'] as gift,
    coalesce(attributes['device'], 'direct') as device_or_default,
    attributes['utm_source'] as utm_source
from carts
