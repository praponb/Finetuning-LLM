with filtered as (
    select
        *,
        substring(channel, 1, 4) as channel_prefix,
        datediff(current_date(), order_date) as days_since_order_date
    from orders
    where order_date > date '2023-06-03'
      and channel is not null
)
select *
from (
    select
        id,
        status,
        discount,
        shipping_fee,
        amount
    from filtered
) src
unpivot (
    reading for kpi in (discount, shipping_fee, amount)
);
