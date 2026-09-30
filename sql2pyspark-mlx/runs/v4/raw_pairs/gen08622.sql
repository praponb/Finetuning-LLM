SELECT a.customer_id, a.opened_date, t.txn_type, LOWER(a.status) AS status_lower FROM accounts a INNER JOIN transactions t ON a.id = t.account_id ORDER BY txn_type ASC;
