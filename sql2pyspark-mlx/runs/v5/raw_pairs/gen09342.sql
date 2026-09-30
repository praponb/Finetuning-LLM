select a.id, a.balance from accounts a where a.customer_id in (select id from customers where credit_limit <= 10800)
