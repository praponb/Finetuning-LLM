select s.customer_id, concat_ws(', ', sort_array(collect_set(we.event_type))) as event_type_values
from web_events we
inner join sessions s on we.session_id = s.id
group by s.customer_id
order by event_type_values, customer_id
limit 5
