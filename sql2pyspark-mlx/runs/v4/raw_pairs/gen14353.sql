SELECT t.merchant, t.is_flagged, a.customer_id FROM transactions t INNER JOIN accounts a ON t.account_id = a.id WHERE a.account_type = 'SAVINGS'
