with lines as (select id, created_at, coupon_codes, inline(items) from carts) select created_at, coupon_codes, product_id, qty, price, promo from lines
