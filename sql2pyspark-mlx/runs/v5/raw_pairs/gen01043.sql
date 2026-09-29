with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        method,
        paid_at,
        status,
        round(amount * 100, 2) adjusted_amount,
        upper(method) method_upper,
        coalesce(method, 'DIRECT') method_clean
    from payments
    where paid_at >= '2023-04-01'
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(paid_at) over (partition by status order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by status order by paid_at, id) + interval 30 days then 1 else 0 end as new_group_flag
    from base
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as island_id
    from gaps
),
gaps_2 as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by status order by paid_at, id) > interval '60' day then 1 else 0 end as new_group_flag_2
    from islands
),
sessions_payments as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag_2) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as session_id
    from gaps_2
),
with_prev as (
    -- Previous event time per key
    select *, lag(paid_at) over (partition by method order by paid_at, id) as prev_paid_at
    from sessions_payments
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_paid_at is null or paid_at > prev_paid_at + interval '7' day then 1 else 0 end as is_new_session
    from with_prev
),
sessions_payments_2 as (
    -- Running sum of the flag = session number
    select
        id,
        order_id,
        method,
        paid_at,
        adjusted_amount,
        method_upper,
        session_id,
        sum(is_new_session) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as island_id_2
    from with_gaps
)
-- Grouped metrics
select
    date_trunc('year', paid_at) as year_start,
    (unix_timestamp(max(paid_at)) - unix_timestamp(min(paid_at))) / 60 as duration_min,
    count_if(method = 'paypal') as paypal_events,
    sum(case when method = 'card' then adjusted_amount else 0 end) as card_adjusted_amount,
    count(*) filter (where method = 'paypal') as paypal_rows
from sessions_payments_2
group by date_trunc('year', paid_at)
order by duration_min;
