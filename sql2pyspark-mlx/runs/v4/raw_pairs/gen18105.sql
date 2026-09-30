with base as (
    select
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        round(duration_sec, 1) as duration_sec_rounded
    from web_events
    where page = '/search'
),
matched_keys as (
    select id
    from base
    except
    select id
    from web_events
    where page in ('/checkout', '/cart')
),
kept_rows as (
    select s.*
    from base s
    inner join matched_keys k on s.id = k.id
),
enriched as (
    select
        *,
        percent_rank() over w_recent as duration_sec_rounded_percent_rank,
        sum(duration_sec_rounded) over w_recent_2 as session_id_sum_duration_sec_rounded,
        avg(duration_sec) over w_recent_2 as session_id_avg_duration_sec
    from kept_rows
    window w_recent as (partition by page, session_id order by duration_sec_rounded nulls first), w_recent_2 as (partition by session_id)
)
select
    case when session_id_sum_duration_sec_rounded >= 409 then 'PREMIUM' else 'STANDARD' end as session_id_sum_duration_sec_rounded_tier,
    case when duration_sec_rounded_percent_rank >= 0.81 then 'HIGH' else 'LOW' end as duration_sec_rounded_percent_rank_tier,
    approx_percentile(duration_sec_rounded, 0.9) as duration_sec_rounded_p90,
    size(collect_set(page)) as num_pages,
    sort_array(collect_set(page)) as page_list,
    grouping_id() as gid
from enriched
group by grouping sets ((case when session_id_sum_duration_sec_rounded >= 409 then 'PREMIUM' else 'STANDARD' end, case when duration_sec_rounded_percent_rank >= 0.81 then 'HIGH' else 'LOW' end), (case when session_id_sum_duration_sec_rounded >= 409 then 'PREMIUM' else 'STANDARD' end), ());
