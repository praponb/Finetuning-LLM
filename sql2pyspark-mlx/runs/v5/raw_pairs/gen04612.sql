with filtered as (
    -- Basic cleansing and defaults
    select *, date_format(paid_at, 'MMM yyyy') as paid_at_label
    from payments
    where paid_at > date '2023-05-13'
      and order_id is not null
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by status order by paid_at, id) > interval '30' day then 1 else 0 end as new_group_flag
    from filtered
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        order_id,
        amount,
        paid_at,
        status,
        paid_at_label,
        sum(new_group_flag) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as session_no
    from flagged
),
with_dims as (
    -- Attach orders attributes
    select
        s.id,
        s.order_id,
        s.session_no,
        o.order_date
    from islands s
    left join orders o
        on s.order_id <=> o.id
    where o.status != 'SHIPPED'
),
kept_keys as (
    -- Set intersection on keys
    select order_id
    from with_dims
    intersect
    select order_id
    from payments
    where to_date(paid_at) < '2025-09-01'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_dims s
    inner join kept_keys k on s.order_id = k.order_id
)
-- Aggregate with conditional metrics
select
    date_trunc('week', order_date) as week_start,
    order_id,
    date_trunc('quarter', order_date) as quarter_start,
    max(session_no) as max_session_no,
    round(avg(session_no), 2) as avg_session_no,
    round(avg(session_no), 2) as avg_session_no_2,
    count(distinct order_id) as distinct_order_ids,
    round(avg(session_no), 2) as avg_session_no_3,
    approx_percentile(session_no, array(0.25, 0.5, 0.75)) as session_no_quartiles
from kept_rows
group by week_start, order_id, quarter_start
having max_session_no > 0
