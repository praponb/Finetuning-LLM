SELECT account_type, customer_id, MIN(opened_date) AS first_opened_date FROM accounts WHERE status IN ('OPEN', 'FROZEN') AND balance BETWEEN 52000 AND 57000 GROUP BY account_type, customer_id
