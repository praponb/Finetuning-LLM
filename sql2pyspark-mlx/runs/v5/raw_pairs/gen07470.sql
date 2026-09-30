WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(status) AS status_length,
        COALESCE(method, 'OTHER') AS resolved_method
    FROM payments
    WHERE paid_at > TO_DATE('2023-11-09')
      AND method IS NOT NULL
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    APPROX_PERCENTILE(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS span_minutes,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(status_length, 0.5) AS status_length_p50
FROM filtered
GROUP BY DATE_TRUNC('YEAR', paid_at)
HAVING (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 > 0
ORDER BY year_start DESC;
