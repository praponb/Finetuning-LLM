select
    p.country,
    e.page,
    count(*) as cnt,
    round(sum(e.duration_sec), 2) as total_duration_sec
from web_events e
left join sessions p on e.session_id = p.id
group by p.country, e.page
