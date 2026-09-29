with ranked as (
    select *, row_number() over (partition by event_type order by event_time desc nulls last, id) as row_num
    from web_events
    qualify row_num = 1
),
enriched as (
    select
        src.*,
        s.browser,
        s.country,
        s.device
    from ranked src
    left join sessions s
        on src.session_id = s.id
),
with_prev as (
    select *, lag(event_time) over (partition by page order by event_time, id) as prev_event_time
    from enriched
),
gaps as (
    select *, case when prev_event_time is null or event_time > prev_event_time + interval 14 days then 1 else 0 end as session_start_flag
    from with_prev
),
sessions_web_events as (
    select *, sum(session_start_flag) over (partition by page order by event_time, id rows between unbounded preceding and current row) as island_id
    from gaps
)
select
    case when island_id >= 8 then 'HIGH' else 'LOW' end as island_id_tier,
    browser,
    sum(duration_sec) as total_duration_sec,
    sum(case when event_type = 'click' then session_start_flag else 0 end) as click_session_start_flag,
    percentile_approx(session_start_flag, array(0.25, 0.5, 0.75)) as session_start_flag_quartiles,
    percent_rank() over (partition by case when island_id >= 8 then 'HIGH' else 'LOW' end order by sum(case when event_type = 'click' then session_start_flag else 0 end) desc) as click_session_start_flag_percent_rank
from sessions_web_events
group by case when island_id >= 8 then 'HIGH' else 'LOW' end, browser
order by island_id_tier desc nulls first;
