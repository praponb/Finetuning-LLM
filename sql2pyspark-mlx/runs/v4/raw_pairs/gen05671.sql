WITH valid_transactions AS (
    SELECT *
    FROM transactions
    WHERE is_flagged OR amount >= 2400
),
Summary AS (
    SELECT CASE WHEN YEAR(txn_time) < 2024 THEN 'before_2024' WHEN YEAR(txn_time) = 2024 THEN '2024' ELSE 'after_2024' END AS txn_cohort, SUM(CASE WHEN txn_type = 'FEE' THEN 1 ELSE 0 END) AS fee_count
    FROM valid_transactions
    GROUP BY CASE WHEN YEAR(txn_time) < 2024 THEN 'before_2024' WHEN YEAR(txn_time) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT txn_cohort, fee_count
FROM Summary
ORDER BY txn_cohort ASC;
