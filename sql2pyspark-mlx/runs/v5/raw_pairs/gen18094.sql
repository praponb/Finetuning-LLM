with by_dimension as (
    select
        d.browser,
        e.page,
        count(*) as cnt,
        avg(e.duration_sec) as avg_duration_sec,
        grouping_id() as level_id
    from web_events e
    inner join sessions d on e.session_id = d.id
    group by grouping sets ((d.browser, e.page), (d.browser), ())
)
select
    browser,
    page,
    cnt,
    avg_duration_sec,
    level_id,
    case when browser rlike '^(Chrome|Firefox)$' then 'CORE' when browser rlike '^(Safari|Edge)$' then 'EXTENDED' else 'GROUP_A' end as browser_zone
from by_dimension
