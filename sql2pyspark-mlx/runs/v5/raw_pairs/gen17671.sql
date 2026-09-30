select a.id, a.balance, a.account_type, a.customer_id from accounts a where a.customer_id not in (select id from customers where signup_date > '2023-03-01')
