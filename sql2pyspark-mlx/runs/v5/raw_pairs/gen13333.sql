select
    case when year(event_time) < 2024 then 'before_2024' when year(event_time) = 2024 then '2024' else 'after_2024' end as event_cohort,
    page,
    round(avg(duration_sec), 2) as avg_duration_sec,
    sum(case when event_type = 'purchase' then duration_sec else 0 end) as purchase_duration_sec
from web_events
where event_type in ('page_view', 'search') and page != '/cart'
group by 1, 2
order by avg_duration_sec, event_cohort, page
limit 10
