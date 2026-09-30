select concat_ws(', ', sort_array(collect_set(page))) as page_values, count(case when event_type = 'purchase' then 1 end) as purchase_count
from web_events
where page = '/search';
