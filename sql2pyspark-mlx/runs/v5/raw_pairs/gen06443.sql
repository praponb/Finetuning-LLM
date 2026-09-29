select sessions.browser, min(web_events.duration_sec) as min_duration_sec
from web_events
inner join sessions on web_events.session_id = sessions.id
inner join customers on sessions.customer_id = customers.id
group by sessions.browser
having min(web_events.duration_sec) > 160
order by min_duration_sec desc, sessions.browser
limit 3
