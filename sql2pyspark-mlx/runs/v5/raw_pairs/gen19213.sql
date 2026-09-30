with lines as (
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        v
    from carts
    lateral view explode(attributes) e as key, v
)
select
    s.id,
    s.customer_id,
    s.created_at,
    s.key,
    s.v,
    (select min(x.balance) from accounts x where x.customer_id = s.customer_id) as min_account_balance
from lines s;
