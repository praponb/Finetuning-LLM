WITH cleaned AS (
    SELECT
        id,
        amount,
        paid_at,
        FLOOR(amount) AS amount_whole,
        INITCAP(TRIM(method)) AS method_title
    FROM payments
    WHERE status NOT IN ('SUCCESS', 'FAILED', 'REFUNDED')
),
metrics AS (
    SELECT
        *,
        SUM(amount) OVER (ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        AVG(amount_whole) OVER (ORDER BY amount_whole, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS amount_whole_avg_5,
        FIRST_VALUE(amount_whole) IGNORE NULLS OVER (ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount_whole,
        NTILE(4) OVER (ORDER BY amount DESC, id) AS amount_bucket
    FROM cleaned
),
valid AS (
    SELECT
        id,
        amount,
        paid_at,
        amount_whole,
        method_title,
        running_amount,
        amount_whole_avg_5,
        first_known_amount_whole,
        amount_bucket,
        COALESCE(amount_whole_avg_5, 0) AS amount_whole_avg_5_filled
    FROM metrics
    WHERE paid_at >= DATE '2023-05-27'
)
SELECT
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    CASE WHEN amount_whole_avg_5_filled >= 84 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_whole_avg_5_filled_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(paid_at) AS ended_at,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS span_minutes,
    MAX(paid_at) AS ended_at_2,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM valid
GROUP BY DATE_TRUNC('YEAR', paid_at), CASE WHEN amount_whole_avg_5_filled >= 84 THEN 'PREMIUM' ELSE 'STANDARD' END
