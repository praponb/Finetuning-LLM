select s.browser, count(case when we.event_type = 'add_to_cart' then 1 end) as add_to_cart_count
from web_events we
left join sessions s on we.session_id = s.id
left join customers c on s.customer_id = c.id
where c.city not in ('Bangkok', 'Singapore')
group by s.browser
having count(case when we.event_type = 'add_to_cart' then 1 end) > 4
