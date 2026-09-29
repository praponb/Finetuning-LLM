select
    s.id,
    s.customer_id,
    s.device,
    s.country,
    top1.web_event_event_type,
    top1.page
from sessions s
left join lateral (
        select c.event_type as web_event_event_type, c.page
        from web_events c
        where c.session_id = s.id
        order by c.event_time desc, c.id desc
        limit 1
    ) top1 on true;
