select c.age, s.monthly_fee, s.plan from customers c left join subscriptions s on s.customer_id = c.id where s.start_date <= '2025-03-13'
