with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        amount * shipping_fee as amount_with_shipping,
        coalesce(channel, 'N/A') as channel_clean
    from orders
    where status = 'COMPLETED'
)
-- Grouped metrics
select
    status,
    date_trunc('year', order_date) as year_start,
    date_trunc('week', order_date) as week_start,
    sum(case when status = 'SHIPPED' then amount_with_shipping else 0 end) as shipped_amount_with_shipping,
    percentile_approx(amount, 0.95) as amount_p95,
    round(avg(amount), 2) as avg_amount,
    grouping_id() as grouping_id
from base
group by grouping sets ((status, date_trunc('year', order_date), date_trunc('week', order_date)), (status, date_trunc('year', order_date)))
order by week_start desc;
