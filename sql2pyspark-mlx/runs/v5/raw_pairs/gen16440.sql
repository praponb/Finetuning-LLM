select s.device, s.customer_id, we.event_time
from sessions s
left join web_events we on we.session_id = s.id
where s.browser in ('Firefox', 'Safari') and we.session_id in (8, 34)
order by device;
