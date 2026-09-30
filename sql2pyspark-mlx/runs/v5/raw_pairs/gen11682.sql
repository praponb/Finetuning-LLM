with recent_web_events as (
    select *
    from web_events
    where event_type <> 'click'
),
event_month_summary as (
    select date_format(event_time, 'yyyy-MM') as event_month, round((100 * sum(case when page = '/product' then 1 else 0 end)) / count(*), 1) as pct_product, max(duration_sec) - min(duration_sec) as duration_sec_range
    from recent_web_events
    group by date_format(event_time, 'yyyy-MM')
)
select *
from event_month_summary
order by duration_sec_range desc, 2 desc
