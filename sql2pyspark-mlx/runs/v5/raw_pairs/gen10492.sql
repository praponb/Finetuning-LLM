WITH base AS (
    SELECT *, LOWER(status) status_lower
    FROM payments
    WHERE HOUR(paid_at) BETWEEN 6 AND 10 AND status NOT IN ('FAILED', 'REFUNDED')
),
paid_month_summary AS (
    SELECT
        DATE_TRUNC('MONTH', paid_at) paid_month,
        CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END paid_cohort,
        ROUND((100 * SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_failed,
        ROUND(STDDEV(amount), 2) stddev_amount
    FROM base
    GROUP BY DATE_TRUNC('MONTH', paid_at), CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT
    paid_month,
    paid_cohort,
    pct_failed,
    stddev_amount
FROM paid_month_summary
WHERE stddev_amount > 62
