with flattened as (select id, customer_id, explode_outer(coupon_codes) as element from carts) select element from flattened order by element nulls last;
