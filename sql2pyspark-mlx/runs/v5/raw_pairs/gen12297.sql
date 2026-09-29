with base as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        case when status = 'REFUNDED' then 1 else 0 end as is_refunded,
        coalesce(method, 'NONE') as resolved_method
    from payments
    where status <> 'FAILED'
),
with_dims as (
    -- Attach orders attributes
    select a.*, o.order_date
    from base a
    inner join orders o
        on a.order_id = o.id
),
scored as (
    -- Ranking and gap features
    select
        *,
        count(amount) over w_part as running_count_amount,
        avg(amount) over w_part as running_avg_amount,
        sum(amount) over w_part as running_sum_amount,
        sum(is_refunded) over w_part_2 as running_is_refunded
    from with_dims
    window w_part as (partition by order_id order by amount asc, id asc rows between unbounded preceding and current row), w_part_2 as (partition by method order by amount asc, id asc rows between unbounded preceding and current row)
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by method order by paid_at asc, id asc) > interval 30 days then 1 else 0 end as session_start_flag
    from scored
),
sessions_payments as (
    -- Running sum of the flag = session number
    select
        id,
        method,
        paid_at,
        status,
        is_refunded,
        sum(session_start_flag) over (partition by method order by paid_at asc, id asc rows between unbounded preceding and current row) as island_id
    from flagged
),
scored_payments as (
    -- Ranking and gap features
    select
        *,
        max(is_refunded) over (partition by status order by island_id asc, id asc rows between unbounded preceding and current row) as running_max_is_refunded,
        min(is_refunded) over (partition by status order by island_id asc, id asc rows between unbounded preceding and current row) as running_min_is_refunded,
        avg(is_refunded) over (partition by status order by island_id asc, id asc rows between unbounded preceding and current row) as running_avg_is_refunded,
        sum(is_refunded) over (partition by status order by island_id asc, id asc rows between unbounded preceding and current row) as running_sum_is_refunded,
        ntile(3) over (partition by status order by island_id asc, id asc) as island_id_bucket
    from sessions_payments
    where status not in ('SUCCESS', 'FAILED')
)
-- Aggregate with conditional metrics
select
    date_trunc('week', paid_at) as week_start,
    min(paid_at) as started_at,
    count(distinct status) as distinct_status
from scored_payments
group by date_trunc('week', paid_at)
order by distinct_status asc nulls first
