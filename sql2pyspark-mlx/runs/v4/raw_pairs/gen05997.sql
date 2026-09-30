select o.id, o.customer_id, s.store_name
from orders o
inner join stores s on s.id = o.store_id
where s.city = 'Bangkok';
