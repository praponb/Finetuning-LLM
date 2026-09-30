select
    page,
    case when duration_sec >= 530 then 'HIGH' else 'LOW' end as duration_sec_tier,
    max(event_time) as last_event_time,
    count(*) filter (where event_type = 'purchase') as purchase_rows,
    round(sum(duration_sec) / nullif(count(case when page = '/checkout' then 1 end), 0), 2) as duration_sec_per_checkout,
    stddev_samp(duration_sec) as duration_sec_stddev,
    count_if(event_type = 'click') as click_events,
    approx_percentile(duration_sec, array(0.25, 0.5, 0.75)) as duration_sec_quartiles,
    grouping(page) as grouping_level
from web_events
group by grouping sets ((page, case when duration_sec >= 530 then 'HIGH' else 'LOW' end), (page))
order by duration_sec_per_checkout desc nulls first
