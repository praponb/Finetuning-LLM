WITH recent_payments AS (
    SELECT *
    FROM payments
    WHERE amount IS NOT NULL
),
status_summary AS (
    SELECT
        status,
        CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END AS paid_cohort,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) AS method_values,
        MIN(paid_at) AS min_paid_at
    FROM recent_payments
    GROUP BY status, CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT
    status,
    paid_cohort,
    method_values,
    min_paid_at
FROM status_summary
ORDER BY status;
