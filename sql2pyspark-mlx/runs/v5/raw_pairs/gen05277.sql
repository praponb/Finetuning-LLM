with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        code
    from carts
    lateral view explode(coupon_codes) x as code
)
select
    s.id,
    s.customer_id,
    s.code,
    case when code rlike '^(SPRING10|VIP)$' then 'EXTENDED' when code rlike '^(BUNDLE|WELCOME5)$' then 'PRIORITY' else 'GROUP_A' end as code_zone,
    (select min(t2.monthly_fee) from subscriptions t2 where t2.customer_id = s.customer_id) as min_subscription_monthly_fee
from flattened s
order by customer_id;
