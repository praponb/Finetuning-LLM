select s.id, s.monthly_fee, s.customer_id, s.plan from subscriptions s where s.customer_id in (select id from customers where credit_limit >= 15500);
