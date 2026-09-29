SELECT CASE WHEN merchant IN ('7-Eleven', 'Lazada') THEN 'core' ELSE 'other' END AS merchant_group, DATE_TRUNC('MONTH', txn_time) AS txn_month, ROUND((100 * SUM(CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_credit
FROM transactions
WHERE is_flagged AND txn_type != 'CREDIT' AND is_flagged = FALSE
GROUP BY CASE WHEN merchant IN ('7-Eleven', 'Lazada') THEN 'core' ELSE 'other' END, DATE_TRUNC('MONTH', txn_time)
