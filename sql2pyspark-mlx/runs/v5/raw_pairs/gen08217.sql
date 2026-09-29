SELECT
    is_flagged,
    TO_DATE(txn_time) AS txn_day,
    AVG(amount) AS avg_amount,
    ROUND(STDDEV(amount), 2) AS stddev_amount
FROM transactions
WHERE merchant IN ('Amazon', 'Shell') AND amount > 2800
GROUP BY is_flagged, txn_day
HAVING avg_amount < 3300;
