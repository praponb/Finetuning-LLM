SELECT id, customer_id, opened_date, status, FIRST_VALUE(status) OVER (PARTITION BY customer_id ORDER BY opened_date, id) AS first_status FROM accounts WHERE account_type != 'CHECKING'
