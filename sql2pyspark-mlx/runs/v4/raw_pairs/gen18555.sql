select *
from (
    select case when page in ('/search', '/checkout') then 'priority' else 'other' end as page_group, round((100 * sum(case when event_type = 'purchase' then 1 else 0 end)) / count(*), 1) as pct_purchase
    from web_events
    where to_date(event_time) < '2023-04-23'
    group by case when page in ('/search', '/checkout') then 'priority' else 'other' end
) as agg
where pct_purchase > 58
order by page_group desc
