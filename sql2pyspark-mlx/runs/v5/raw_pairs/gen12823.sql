SELECT id, balance FROM accounts WHERE NOT (status = 'CLOSED' AND YEAR(opened_date) = 2024 AND customer_id IS NOT NULL)
