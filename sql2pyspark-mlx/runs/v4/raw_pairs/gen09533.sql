SELECT
    account_id,
    CASE WHEN amount >= 4000 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    MAX(txn_time) AS last_txn_time,
    MAX(amount) FILTER (WHERE txn_type = 'CREDIT') AS credit_max_amount,
    SUM(CASE WHEN txn_type = 'CREDIT' THEN amount ELSE 0 END) AS credit_amount,
    COUNT(*) AS row_count,
    RANK() OVER (PARTITION BY account_id ORDER BY COUNT(*) DESC) AS row_count_rank
FROM transactions
GROUP BY account_id, CASE WHEN amount >= 4000 THEN 'PREMIUM' ELSE 'STANDARD' END;
