with flagged as (
    select *, case when started_at - lag(started_at) over (partition by browser order by started_at, id) > interval '14' day then 1 else 0 end as session_start_flag
    from sessions
),
sessions_sessions as (
    select
        id,
        browser,
        started_at,
        country,
        sum(session_start_flag) over (partition by browser order by started_at, id rows between unbounded preceding and current row) as burst_id
    from flagged
)
select
    case when burst_id >= 7 then 'LARGE' else 'SMALL' end as burst_id_tier,
    date_trunc('week', started_at) as week_start,
    round(avg(burst_id), 2) as avg_burst_id,
    (unix_timestamp(max(started_at)) - unix_timestamp(min(started_at))) / 60 as span_minutes,
    min(started_at) as first_started_at,
    size(collect_set(browser)) as num_browsers
from sessions_sessions
group by case when burst_id >= 7 then 'LARGE' else 'SMALL' end, date_trunc('week', started_at)
order by week_start desc
