with scored as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by event_type, session_id order by duration_sec, id) as event_seq,
        avg(duration_sec) over (order by duration_sec, id rows between 3 preceding and 1 preceding) as duration_sec_avg_3_prior,
        sum(duration_sec) over w_grp as duration_sec_sum_90d,
        lead(duration_sec, 2, 0) over (partition by event_type order by event_time, id) as next_duration_sec,
        ntile(5) over w_ordered as duration_sec_bucket
    from web_events
    window w_grp as (partition by session_id order by cast(event_time as bigint) range between 7776000 preceding and current row), w_ordered as (partition by session_id, page order by duration_sec, id)
),
enriched as (
    -- Attach sessions attributes
    select
        e.id,
        e.session_id,
        e.duration_sec_avg_3_prior,
        e.next_duration_sec,
        e.duration_sec_bucket,
        s.browser
    from scored e
    left join sessions s
        on e.session_id = s.id
),
with_dims as (
    -- Attach sessions attributes
    select
        e.id,
        e.session_id,
        e.next_duration_sec,
        d.browser as session_browser
    from enriched e
    inner join sessions d
        on e.session_id = d.id
),
valid as (
    -- Keep recent, valid rows
    select *, case when next_duration_sec >= 452 then 'High' when next_duration_sec >= 94 then 'Medium' else 'Low' end as next_duration_sec_level
    from with_dims
    where session_browser is not null
      and session_browser = 'Safari'
),
enriched_web_events as (
    -- Running and moving aggregates
    select
        id,
        session_id,
        min(next_duration_sec) over w_grp as running_min_next_duration_sec,
        sum(next_duration_sec) over w_grp as running_sum_next_duration_sec,
        count(next_duration_sec) over w_grp as running_count_next_duration_sec,
        avg(next_duration_sec) over w_grp as running_avg_next_duration_sec
    from valid
    window w_grp as (partition by session_browser order by next_duration_sec, id rows between unbounded preceding and current row)
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        coalesce(running_count_next_duration_sec, 0) as running_count_next_duration_sec_filled,
        cast(running_sum_next_duration_sec as int) as running_sum_next_duration_sec_int
    from enriched_web_events
    where running_min_next_duration_sec > 240
)
-- Aggregate with conditional metrics
select
    case when running_min_next_duration_sec >= 410 then 'PREMIUM' else 'STANDARD' end as running_min_next_duration_sec_tier,
    case when running_count_next_duration_sec >= 17 then 'PREMIUM' else 'STANDARD' end as running_count_next_duration_sec_tier,
    count(distinct id) as distinct_ids,
    round(avg(running_sum_next_duration_sec), 2) as avg_running_sum_next_duration_sec,
    percentile_approx(running_avg_next_duration_sec, 0.9) as running_avg_next_duration_sec_p90
from base
group by 1, 2
having percentile_approx(running_avg_next_duration_sec, 0.9) < 1000000
order by running_avg_next_duration_sec_p90;
