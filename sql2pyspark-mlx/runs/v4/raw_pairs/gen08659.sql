with base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        coalesce(discount, 0) as discount_filled,
        coalesce(channel, 'DIRECT') as channel_filled
    from orders
    where order_date > date '2023-04-13'
)
-- Native SQL PIVOT
select *
from (
    select
        status,
        channel,
        discount_filled
    from base
) src
pivot (
    max(discount_filled) as amount,
    count(1) as n
    for channel in ('mobile', 'store', 'partner')
);
