SELECT id, opened_date FROM accounts WHERE account_type NOT IN ('LOAN', 'CHECKING') UNION SELECT id, opened_date FROM accounts WHERE customer_id IS NOT NULL
