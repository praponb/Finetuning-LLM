SELECT id, account_type
FROM accounts
QUALIFY RANK() OVER (PARTITION BY customer_id ORDER BY balance DESC) = 1;
