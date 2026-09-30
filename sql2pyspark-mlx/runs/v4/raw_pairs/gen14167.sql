select o.order_date, c.city, s.region
from orders o
left join customers c on o.customer_id = c.id
join stores s on s.id = o.store_id
where month(o.order_date) in (4, 9, 10) or o.amount <= 540
