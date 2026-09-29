select c.is_active, c.segment, c.city, s.end_date from customers c inner join subscriptions s on s.customer_id = c.id where s.customer_id in (21, 34);
