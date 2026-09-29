with flattened as (select id, customer_id, created_at, value from carts lateral view explode(coupon_codes) t as value) select customer_id from flattened order by customer_id nulls last;
