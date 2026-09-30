select
    s.id,
    s.started_at,
    s.browser,
    s.country
from sessions s
where s.id in (select session_id from web_events where duration_sec between 256 and 421)
order by s.started_at asc;
