SELECT
    is_flagged,
    merchant,
    ROUND((100 * SUM(CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_credit,
    COUNT(amount) AS amount_count,
    MIN(amount) AS min_amount
FROM transactions
WHERE txn_time < '2024-07-01' AND is_flagged
GROUP BY is_flagged, merchant
ORDER BY amount_count DESC;
