select c.is_active, min(o.order_date) as min_order_date
from orders o
right join customers c on o.customer_id = c.id
inner join stores s on o.store_id = s.id
where o.store_id = 5
group by c.is_active
order by 2 desc;
