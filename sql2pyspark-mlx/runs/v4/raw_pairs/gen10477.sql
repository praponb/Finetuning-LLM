SELECT customer_id, status FROM accounts WHERE status != 'CLOSED' AND status IN ('CLOSED', 'FROZEN', 'OPEN') AND YEAR(opened_date) >= 2023
