select
    id,
    customer_id,
    created_at,
    coupon_codes,
    attributes,
    array_join(array_sort(coupon_codes), ',') as coupons_text,
    size(attributes) as num_attributes,
    coalesce(attributes['gift'], 'direct') as gift_or_default
from carts
where customer_id = 5;
