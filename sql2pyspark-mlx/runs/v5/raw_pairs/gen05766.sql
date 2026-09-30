select sessions.device, sort_array(collect_set(web_events.event_type)) as event_type_list
from web_events
left join sessions on web_events.session_id = sessions.id
left join customers on customers.id = sessions.customer_id
group by sessions.device;
