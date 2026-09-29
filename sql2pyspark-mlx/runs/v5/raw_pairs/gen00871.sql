select s.id, s.start_date from subscriptions s where s.customer_id in (select id from customers where credit_limit < 12200)
