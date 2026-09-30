with latest as (
    select *
    from (
        select *, row_number() over (partition by event_type order by event_time nulls last, id) as latest_rank
        from web_events
    ) ranked
    where latest_rank = 1
),
valid as (
    select
        *,
        round(latest_rank * 1.1, 2) as latest_rank_adjusted,
        coalesce(page, 'UNKNOWN') as resolved_page
    from latest
    where event_time >= to_timestamp('2023-06-09 22:00:00')
),
valid_web_events as (
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        latest_rank,
        latest_rank_adjusted,
        resolved_page,
        cast(event_time as date) as event_time_day,
        duration_sec - latest_rank as duration_sec_minus_latest_rank
    from valid
    where page is distinct from '/cart'
),
matched_keys as (
    select session_id
    from valid_web_events
    intersect
    select session_id
    from web_events
    where session_id in (4, 22, 27)
),
verified as (
    select s.*
    from valid_web_events s
    inner join matched_keys k on s.session_id = k.session_id
),
kept_keys as (
    select page
    from verified
    except distinct
    select page
    from web_events
    where page in ('/home', '/cart', '/checkout')
),
kept_rows as (
    select s.*
    from verified s
    inner join kept_keys k on s.page = k.page
),
enriched as (
    select
        src.session_id,
        src.event_type,
        src.latest_rank,
        src.latest_rank_adjusted,
        src.event_time_day,
        s.device
    from kept_rows src
    left join sessions s
        on src.session_id = s.id
    where s.browser is null
),
scored as (
    select
        session_id,
        event_type,
        latest_rank,
        device,
        max(latest_rank_adjusted) over (partition by session_id, device) as session_id_max_latest_rank_adjusted,
        cume_dist() over (partition by device order by latest_rank_adjusted nulls first) as latest_rank_adjusted_cume_dist
    from enriched
)
select
    device,
    case when latest_rank_adjusted_cume_dist >= 0.86 then 'PREMIUM' else 'STANDARD' end as latest_rank_adjusted_cume_dist_tier,
    session_id,
    count(*) filter (where event_type = 'page_view') as page_view_rows,
    count(distinct event_type) as distinct_event_types,
    max(latest_rank_adjusted_cume_dist) filter (where device = 'desktop') as desktop_max_latest_rank_adjusted_cume_dist,
    round(avg(session_id_max_latest_rank_adjusted), 2) as avg_session_id_max_latest_rank_adjusted,
    min(latest_rank_adjusted_cume_dist) as min_latest_rank_adjusted_cume_dist,
    grouping(device) as grouping_id
from scored
group by rollup (device, case when latest_rank_adjusted_cume_dist >= 0.86 then 'PREMIUM' else 'STANDARD' end, session_id)
having min(latest_rank_adjusted_cume_dist) > 0
order by desktop_max_latest_rank_adjusted_cume_dist nulls last
