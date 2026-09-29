SELECT a.id, a.status, a.opened_date, a.customer_id FROM accounts a WHERE a.id IN (SELECT account_id FROM transactions WHERE merchant IN ('Shell', 'Grab'))
