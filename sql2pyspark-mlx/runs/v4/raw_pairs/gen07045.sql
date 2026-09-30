WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(status) AS status_lower
    FROM payments
    WHERE paid_at >= TO_DATE('2023-11-03')
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(method) IGNORE NULLS OVER (PARTITION BY method ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_method,
        FIRST_VALUE(method) IGNORE NULLS OVER (ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_method,
        SUM(amount) OVER (ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM base
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.*,
        d.amount AS order_amount,
        d.order_date
    FROM metrics src
    LEFT JOIN orders d
        ON src.order_id = d.id
)
-- Multi-level aggregation
SELECT
    order_id,
    method,
    status,
    COUNT(*) AS num_rows,
    MAX(paid_at) AS last_seen,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    COUNT_IF(method = 'cash') AS cash_events,
    MIN(paid_at) AS first_paid_at,
    DENSE_RANK() OVER (PARTITION BY order_id ORDER BY COUNT_IF(method = 'cash') DESC) AS cash_events_dense_rank
FROM with_dims
GROUP BY order_id, method, status
