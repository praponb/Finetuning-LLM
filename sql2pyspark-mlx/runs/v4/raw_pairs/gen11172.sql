SELECT a.opened_date, t.txn_time, t.is_flagged, t.amount FROM accounts a LEFT JOIN transactions t ON a.id = t.account_id WHERE a.customer_id IS NULL ORDER BY amount DESC;
