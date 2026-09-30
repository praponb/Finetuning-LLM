select c.id, c.country, count(s.id) as subscription_count from customers c inner join subscriptions s on c.id = s.customer_id where s.start_date >= '2025-01-20' group by c.id, c.country
