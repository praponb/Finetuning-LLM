SELECT id, txn_type, merchant, amount, ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type, merchant), 3) amount_share FROM transactions
