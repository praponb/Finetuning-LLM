with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        to_date(paid_at) paid_at_date,
        substring(method, 1, 3) method_prefix
    from payments
    where paid_at > to_date('2023-06-08')
      and method is not null
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by order_id order by paid_at asc, id asc) > interval '90' day then 1 else 0 end as new_group_flag
    from cleaned
),
sessions_payments as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by order_id order by paid_at asc, id asc rows between unbounded preceding and current row) as session_no
    from gaps
)
-- Multi-level aggregation
select
    date_trunc('month', paid_at) as activity_month,
    order_id,
    size(collect_set(method)) as num_methods,
    (unix_timestamp(max(paid_at)) - unix_timestamp(min(paid_at))) / 60 as duration_min,
    sum(session_no) as total_session_no,
    size(collect_set(method)) as num_methods_2
from sessions_payments
group by grouping sets ((date_trunc('month', paid_at), order_id), (date_trunc('month', paid_at)))
