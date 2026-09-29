with valid as (
    -- Basic cleansing and defaults
    select
        *,
        date_trunc('MONTH', paid_at) as paid_at_period,
        coalesce(method, 'OTHER') as method_clean
    from payments
    where paid_at > to_timestamp('2023-08-03 23:00:00')
),
enriched as (
    -- Attach orders attributes
    select
        s.id,
        s.order_id,
        s.amount,
        s.status,
        s.paid_at_period,
        s.method_clean,
        d.order_date,
        d.channel
    from valid s
    left join orders d
        on s.order_id = d.id
    where d.customer_id in (10, 27, 31)
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        amount,
        status,
        paid_at_period,
        method_clean,
        channel,
        concat_ws('-', channel, status) as channel_status_key,
        cast(paid_at_period as date) as paid_at_period_day,
        coalesce(channel, 'OTHER') as channel_filled
    from enriched
    where hour(paid_at_period) between 12 and 17
),
joined as (
    -- Attach orders attributes
    select
        src.id,
        src.order_id,
        src.amount,
        src.status,
        src.channel,
        src.paid_at_period_day,
        o.channel as order_channel,
        o.discount,
        o.status as order_status
    from base src
    join orders o
        on src.order_id = o.id
),
latest as (
    -- QUALIFY-style window filter
    select
        id,
        order_id,
        amount,
        status,
        order_status
    from (
        select *, row_number() over (partition by order_id order by amount desc, id) as rn
        from joined
    ) x
    where rn = 1
),
latest_payments as (
    -- QUALIFY-style window filter
    select id, order_status
    from latest
    qualify row_number() over (partition by status order by amount desc nulls first, id) = 1
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        case when order_status = 'RETURNED' then 1 else 0 end as is_returned,
        coalesce(order_status, 'DIRECT') as order_status_filled
    from latest_payments
    where order_status is not distinct from 'COMPLETED'
)
select id, order_status
from cleaned
order by id
limit 10;
