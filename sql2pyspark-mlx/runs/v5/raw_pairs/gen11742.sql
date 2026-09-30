select
    s.device,
    s.browser,
    a.page,
    count(*) as rows_total,
    round(avg(a.duration_sec), 2) as avg_duration_sec
from web_events a
inner join sessions s on a.session_id = s.id
group by rollup (s.device, s.browser, a.page)
order by device nulls last
