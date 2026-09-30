WITH base AS (
    SELECT *, INITCAP(TRIM(status)) status_title
    FROM payments
    WHERE amount >= 620 AND TO_DATE(paid_at) < '2025-08-08' AND amount > 880
),
method_summary AS (
    SELECT method, status, ROUND((100 * SUM(CASE WHEN method = 'card' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_card
    FROM base
    GROUP BY method, status
)
SELECT method, status, pct_card
FROM method_summary
ORDER BY pct_card DESC, method, status
LIMIT 1;
