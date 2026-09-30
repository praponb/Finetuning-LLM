select c.city, c.is_active, min(o.order_date) as min_order_date from orders o left join customers c on o.customer_id = c.id group by c.city, c.is_active order by city
