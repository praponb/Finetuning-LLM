with with_windows as (
    select
        *,
        avg(discount) over (order by shipping_fee, id rows between 2 preceding and 1 preceding) discount_avg_2_prior,
        first_value(shipping_fee) ignore nulls over (partition by store_id, channel order by amount, id rows between unbounded preceding and current row) first_known_shipping_fee
    from orders
)
select
    s.id,
    s.customer_id,
    s.order_date,
    s.amount,
    s.discount,
    s.discount_avg_2_prior,
    s.first_known_shipping_fee,
    (select avg(s2.monthly_fee) from subscriptions s2 where s2.customer_id = s.customer_id) as avg_subscription_monthly_fee
from with_windows s
order by customer_id nulls last;
