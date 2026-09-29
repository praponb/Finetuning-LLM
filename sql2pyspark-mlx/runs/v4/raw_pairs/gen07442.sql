select
    event_type,
    session_id,
    case when duration_sec >= 469 then 'HIGH' else 'LOW' end as duration_sec_tier,
    count(distinct id) as distinct_ids,
    percentile_approx(duration_sec, array(0.25, 0.5, 0.75)) as duration_sec_quartiles,
    size(collect_set(page)) as num_pages,
    round(sum(duration_sec) / nullif(count(case when event_type = 'page_view' then 1 end), 0), 2) as duration_sec_per_page_view,
    grouping_id() as grouping_id
from web_events
group by rollup (event_type, session_id, case when duration_sec >= 469 then 'HIGH' else 'LOW' end)
having num_pages >= 1;
