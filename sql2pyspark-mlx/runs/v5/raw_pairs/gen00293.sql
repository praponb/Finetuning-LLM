WITH filtered AS (
    SELECT *, FLOOR(amount) amount_whole
    FROM payments
    WHERE amount < 1220
      AND paid_at >= TIMESTAMP '2023-05-02 03:00:00'
),
deduped AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount_whole ASC NULLS LAST, id ASC) = 1
),
filtered_payments AS (
    SELECT *, CONCAT_WS('/', status, method) status_method_key
    FROM deduped
    WHERE order_id IN (12, 35, 36, 37)
),
metrics AS (
    SELECT
        *,
        SUM(amount) OVER w_recent running_sum_amount,
        COUNT(amount) OVER w_recent running_count_amount,
        MIN(amount) OVER w_recent running_min_amount
    FROM filtered_payments
    WINDOW w_recent AS (PARTITION BY method ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered_payments_2 AS (
    SELECT
        *,
        CASE WHEN method IN ('paypal', 'bank_transfer') THEN 'Group A' ELSE 'Other' END method_group,
        LENGTH(status) status_length
    FROM metrics
    WHERE paid_at > DATE '2023-11-03'
      AND method IS NOT NULL
)
SELECT
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    MAX(paid_at) AS ended_at,
    MIN(paid_at) AS first_seen,
    PERCENTILE_APPROX(running_count_amount, ARRAY(0.25, 0.5, 0.75)) AS running_count_amount_quartiles
FROM filtered_payments_2
GROUP BY DATE_TRUNC('YEAR', paid_at)
ORDER BY year_start DESC
