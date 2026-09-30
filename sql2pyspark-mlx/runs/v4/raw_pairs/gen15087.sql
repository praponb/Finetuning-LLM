with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        started_at,
        country,
        case when device in ('desktop', 'tablet') then 'Tier 1' else 'Other' end as device_group,
        coalesce(country, 'OTHER') as resolved_country
    from sessions
    where started_at >= date '2024-01-28'
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by customer_id order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by customer_id order by started_at, id) + interval '90' day then 1 else 0 end as session_start_flag
    from cleaned
),
sessions_sessions as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by customer_id order by started_at, id rows between unbounded preceding and current row) as session_no
    from gaps
)
-- Multi-level aggregation
select
    case when session_no >= 7 then 'HIGH' else 'LOW' end as session_no_tier,
    case when session_start_flag >= 1 then 'HIGH' else 'LOW' end as session_start_flag_tier,
    (unix_timestamp(max(started_at)) - unix_timestamp(min(started_at))) / 60 as span_minutes,
    size(collect_set(country)) as num_countries,
    percentile_approx(session_start_flag, 0.9) as session_start_flag_p90
from sessions_sessions
group by case when session_no >= 7 then 'HIGH' else 'LOW' end, case when session_start_flag >= 1 then 'HIGH' else 'LOW' end
