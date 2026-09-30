WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        paid_at,
        TO_DATE(paid_at) AS paid_at_date,
        LENGTH(status) AS status_length,
        COALESCE(method, 'NONE') AS resolved_method
    FROM payments
    WHERE paid_at > TO_DATE('2023-12-17')
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY status_length DESC NULLS LAST) AS rn
    FROM base
    QUALIFY rn = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        method,
        paid_at,
        paid_at_date,
        status_length,
        resolved_method,
        rn,
        LAG(status_length, 1) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS prev_status_length,
        LEAD(status_length, 1, 0) OVER (PARTITION BY order_id ORDER BY status_length, id) AS next_status_length,
        SUM(status_length) OVER (PARTITION BY method ORDER BY status_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_length
    FROM deduped
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(running_status_length) OVER (PARTITION BY method, order_id ORDER BY next_status_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_status_length,
        SUM(prev_status_length) OVER (PARTITION BY method ORDER BY status_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_status_length
    FROM metrics
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at_date, id) AS rank_in_group
    FROM enriched
    QUALIFY rank_in_group = 1
)
-- Grouped metrics
SELECT
    order_id,
    PERCENTILE_APPROX(running_running_status_length, 0.95) AS running_running_status_length_p95,
    MIN(paid_at) AS started_at,
    SORT_ARRAY(COLLECT_SET(method)) AS method_list,
    PERCENTILE_APPROX(next_status_length, 0.5) AS next_status_length_p50,
    SUM(CASE WHEN method = 'paypal' THEN running_status_length ELSE 0 END) AS paypal_running_status_length
FROM ranked
GROUP BY order_id
ORDER BY running_running_status_length_p95 NULLS FIRST
