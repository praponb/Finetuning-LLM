select s.id, s.monthly_fee, s.start_date, s.customer_id from subscriptions s where s.customer_id in (select id from customers where age <= 47);
