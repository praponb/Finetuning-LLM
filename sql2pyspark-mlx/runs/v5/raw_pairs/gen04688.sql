SELECT a.account_type, t.txn_time, t.merchant, WEEKOFYEAR(a.opened_date) AS opened_date_weekofyear FROM accounts a LEFT JOIN transactions t ON t.account_id = a.id ORDER BY txn_time
