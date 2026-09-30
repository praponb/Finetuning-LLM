select id, created_at, explode(coupon_codes) as tag from carts
