with lines as (
    select
        id,
        customer_id,
        explode_outer(coupon_codes) as tag
    from carts
)
select
    s.customer_id,
    s.tag,
    (select sum(x.monthly_fee) from subscriptions x where x.customer_id = s.customer_id) as sum_subscription_monthly_fee
from lines s
order by tag desc nulls last, sum_subscription_monthly_fee;
