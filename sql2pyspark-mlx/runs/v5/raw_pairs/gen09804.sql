select c.id, c.email, count(s.id) as session_count
from customers c
left outer join sessions s on c.id = s.customer_id
group by c.id, c.email
order by email asc, session_count desc;
