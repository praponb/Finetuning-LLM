SELECT
    DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_month,
    SUM(amount) AS total_amount,
    COUNT(*) AS cnt,
    SUM(SUM(amount)) OVER (ORDER BY DATE_FORMAT(txn_time, 'yyyy-MM')) AS cumulative_amount
FROM transactions
WHERE txn_type <> 'FEE'
GROUP BY DATE_FORMAT(txn_time, 'yyyy-MM')
