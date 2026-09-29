with with_prev as (
    select *, lag(started_at) over (partition by customer_id order by started_at, id) as prev_started_at
    from sessions
),
with_gaps as (
    select *, case when prev_started_at is null or started_at > prev_started_at + interval 30 days then 1 else 0 end as is_new_burst
    from with_prev
),
sessions_sessions as (
    select *, sum(is_new_burst) over (partition by customer_id order by started_at, id rows between unbounded preceding and current row) as session_id
    from with_gaps
)
select
    date_trunc('quarter', prev_started_at) as quarter_start,
    browser,
    min(prev_started_at) as first_seen,
    max(started_at) as last_started_at
from sessions_sessions
group by quarter_start, browser
order by quarter_start desc;
