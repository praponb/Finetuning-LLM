select
    we.id,
    we.session_id,
    we.page,
    s.country,
    s.browser,
    s.started_at
from web_events we
left outer join sessions s on we.session_id = s.id
where we.page in ('/cart', '/home')
order by we.session_id desc;
