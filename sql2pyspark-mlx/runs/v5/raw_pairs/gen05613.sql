select
    id,
    coupon_codes,
    items,
    ship_to,
    attributes,
    forall(items, li -> li.qty <= 5) as all_small_qty,
    array_max(transform(items, li -> li.price)) as max_item_price
from carts
where array_contains(coupon_codes, 'BUNDLE')
