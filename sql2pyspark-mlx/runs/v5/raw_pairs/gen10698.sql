with flattened as (select id, created_at, explode_outer(coupon_codes) as tag from carts) select id, created_at, tag from flattened;
