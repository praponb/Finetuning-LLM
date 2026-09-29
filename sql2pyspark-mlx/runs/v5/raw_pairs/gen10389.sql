select s.id, s.country
from sessions s
where s.customer_id in (select id from customers where city = 'Bangkok');
