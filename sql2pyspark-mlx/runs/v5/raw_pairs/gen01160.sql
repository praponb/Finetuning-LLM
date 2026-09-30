select customers.city, sort_array(collect_set(sessions.browser)) as browser_list
from sessions
inner join customers on sessions.customer_id = customers.id
where customers.age >= 33
group by customers.city
order by city desc
