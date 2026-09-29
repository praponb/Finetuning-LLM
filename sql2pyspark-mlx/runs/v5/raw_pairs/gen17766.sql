with base as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        case when amount >= 1530 then 'High' when amount >= 380 then 'Medium' else 'Low' end as amount_level,
        coalesce(method, 'NONE') as method_filled
    from payments
    where paid_at > date '2023-09-15'
      and order_id is not null
),
joined as (
    -- Attach orders attributes
    select
        e.id,
        e.order_id,
        e.amount,
        e.paid_at,
        e.amount_level,
        e.method_filled,
        o.order_date,
        o.channel
    from base e
    left join orders o
        on e.order_id = o.id
),
enriched as (
    -- Attach orders attributes
    select a.*, p.channel as order_channel
    from joined a
    left join orders p
        on a.order_id <=> p.id
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(paid_at) over (partition by order_channel order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by order_channel order by paid_at, id) + interval 30 days then 1 else 0 end as session_start_flag
    from enriched
),
sessions_payments as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by order_channel order by paid_at, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
totals as (
    -- Grouped metrics
    select
        order_channel,
        channel,
        case when amount >= 1770 then 'PREMIUM' else 'STANDARD' end as amount_tier,
        round(avg(amount), 2) as avg_amount,
        max(paid_at) as ended_at,
        size(collect_set(channel)) as num_channels,
        round(avg(amount), 2) as avg_amount_2,
        max(burst_id) filter (where channel = 'store') as store_max_burst_id,
        round(avg(amount), 2) as avg_amount_3
    from sessions_payments
    group by rollup (order_channel, channel, case when amount >= 1770 then 'PREMIUM' else 'STANDARD' end)
    having num_channels > 1
)
select
    channel,
    amount_tier,
    avg_amount,
    num_channels,
    avg_amount_2,
    store_max_burst_id,
    avg_amount_3
from totals;
