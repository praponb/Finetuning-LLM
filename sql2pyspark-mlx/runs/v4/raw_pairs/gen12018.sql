SELECT t.id, t.amount, t.merchant, t.is_flagged, a.status FROM transactions t JOIN accounts a ON t.account_id = a.id WHERE t.merchant NOT IN ('Starbucks', 'Amazon', 'Grab') OR a.customer_id IS NULL
