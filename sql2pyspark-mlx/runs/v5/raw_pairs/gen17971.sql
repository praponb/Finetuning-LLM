select o.customer_id, c.age, c.country from orders o right outer join customers c on o.customer_id = c.id;
