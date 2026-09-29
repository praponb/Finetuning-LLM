SELECT t.id, t.merchant, a.balance, a.customer_id, COALESCE(a.balance, 0) balance_filled FROM transactions t LEFT JOIN accounts a ON t.account_id = a.id;
