with prev_events as (
    -- Previous event time per key
    select *, lag(paid_at) over (partition by order_id order by paid_at, id) as prev_paid_at
    from payments
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_paid_at is null or paid_at > prev_paid_at + interval '90' day then 1 else 0 end as is_new_burst
    from prev_events
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        order_id,
        amount,
        paid_at,
        sum(is_new_burst) over (partition by order_id order by paid_at, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(paid_at) over (partition by order_id order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by order_id order by paid_at, id) + interval '7' day then 1 else 0 end as session_start_flag
    from islands
),
islands_payments as (
    -- Running sum of the flag = session number
    select
        id,
        order_id,
        amount,
        paid_at,
        burst_id,
        sum(session_start_flag) over (partition by order_id order by paid_at, id rows between unbounded preceding and current row) as session_id
    from flagged
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by order_id order by paid_at, id) as rank_in_group
    from islands_payments
    qualify rank_in_group = 1
),
joined as (
    -- Attach orders attributes
    select
        e.*,
        b.channel,
        b.order_date,
        b.status
    from deduped e
    left join orders b
        on e.order_id = b.id
),
kept_keys as (
    -- Set intersection on keys
    select order_id, status
    from joined
    intersect all
    select order_id, status
    from payments
    where status in ('SUCCESS', 'REFUNDED', 'FAILED')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from joined s
    join kept_keys k on s.order_id = k.order_id and s.status = k.status
)
-- Join a dimension and aggregate
select
    p.channel as order_channel,
    src.status as payment_status,
    count(*) as num_rows,
    max(src.amount) as max_amount
from reconciled src
left join orders p on src.order_id = p.id
group by p.channel, src.status;
