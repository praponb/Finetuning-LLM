SELECT
    account_id,
    CASE WHEN txn_type IN ('FEE', 'REFUND') THEN 'tier_1' ELSE 'other' END AS txn_type_group,
    MAX(txn_time) AS last_txn_time,
    AVG(amount) AS avg_amount,
    MAX(amount) - MIN(amount) AS amount_range
FROM transactions
GROUP BY 1, 2
HAVING amount_range >= 75;
