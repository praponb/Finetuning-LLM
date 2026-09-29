SELECT id, customer_id, status FROM accounts QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance NULLS FIRST, id) = 1
