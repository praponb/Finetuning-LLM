select id, created_at, coupon_codes, items, ship_to, array_max(transform(items, i -> i.price)) as max_item_price, coalesce(attributes['gift'], 'unknown') as gift_or_default from carts;
