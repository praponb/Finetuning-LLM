SELECT CASE WHEN txn_type IN ('DEBIT', 'REFUND') THEN 'priority' ELSE 'other' END AS txn_type_group, ROUND(STDDEV(amount), 2) AS stddev_amount, COUNT(CASE WHEN txn_type = 'FEE' THEN 1 END) AS fee_count
FROM transactions
WHERE account_id = 3 AND (merchant = 'Shell' OR merchant IN ('Starbucks', 'Amazon'))
GROUP BY txn_type_group
