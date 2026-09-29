select s.id, s.country from sessions s where s.customer_id in (select id from customers where signup_date <= '2024-05-01')
