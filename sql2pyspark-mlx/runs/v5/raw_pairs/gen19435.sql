select s.id, s.customer_id, c.city from sessions s right outer join customers c on s.customer_id = c.id order by customer_id asc
