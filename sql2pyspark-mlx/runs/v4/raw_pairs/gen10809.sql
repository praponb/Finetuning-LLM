SELECT CASE WHEN YEAR(a.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(a.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END opened_cohort, COUNT(*) cnt, ROUND((100 * SUM(CASE WHEN t.txn_type = 'REFUND' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_refund
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
WHERE t.txn_time >= '2024-01-01'
GROUP BY CASE WHEN YEAR(a.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(a.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY pct_refund, opened_cohort;
