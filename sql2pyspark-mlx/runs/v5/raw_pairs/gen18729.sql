SELECT id, account_type
FROM accounts
WHERE customer_id IS NOT NULL AND status = 'OPEN' AND opened_date BETWEEN '2024-05-13' AND '2025-09-01';
