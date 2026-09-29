with valid as (
    -- Keep recent, valid rows
    select
        *,
        amount + shipping_fee as amount_with_shipping,
        substring(status, 1, 2) as status_prefix
    from orders
    where order_date >= to_date('2023-03-21')
),
latest_match as (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.customer_id,
        s.store_id,
        s.status,
        s.channel,
        s.amount,
        s.shipping_fee,
        s.amount_with_shipping,
        x.delivered_date,
        x.shipment_shipped_date
    from valid s
    left join lateral (
            select c.delivered_date, c.shipped_date as shipment_shipped_date
            from shipments c
            where c.order_id = s.id
            order by c.cost desc, c.id desc
            limit 1
        ) x on true
),
with_window as (
    -- Non-equi join on a validity window
    select
        src.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from latest_match src
    left join subscriptions sub
        on src.customer_id = sub.customer_id
       and src.delivered_date >= sub.start_date
       and src.delivered_date < sub.end_date + interval '7' day
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        count(amount_with_shipping) over w_running as last3_count_amount_with_shipping,
        min(amount_with_shipping) over w_running as last3_min_amount_with_shipping,
        sum(amount_with_shipping) over w_running as last3_sum_amount_with_shipping,
        lag(monthly_fee, 1) over (order by monthly_fee, id, subscription_id) as prev_monthly_fee
    from with_window
    where status = 'SHIPPED'
    window w_running as (partition by plan, customer_id order by shipment_shipped_date, id, subscription_id rows between 2 preceding and current row)
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        subscription_id,
        customer_id,
        amount,
        last3_min_amount_with_shipping,
        prev_monthly_fee
    from metrics
) src
unpivot (
    reading for kpi in (amount, last3_min_amount_with_shipping, prev_monthly_fee)
) u
