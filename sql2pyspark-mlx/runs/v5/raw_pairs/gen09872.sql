SELECT
    YEAR(txn_time) AS txn_year,
    SUM(amount) AS total_amount,
    COUNT(*) AS cnt,
    SUM(SUM(amount)) OVER (ORDER BY YEAR(txn_time)) AS cumulative_amount
FROM transactions
WHERE merchant != 'Starbucks'
GROUP BY YEAR(txn_time)
