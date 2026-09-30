WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(amount, 2) AS amount_rounded,
        TO_DATE(paid_at) AS paid_at_date
    FROM payments
    WHERE paid_at > DATE '2023-08-01'
      AND method IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        amount_rounded,
        LEAD(amount_rounded) OVER w_recent AS next_amount_rounded,
        LAG(amount_rounded) OVER w_recent AS prev_amount_rounded,
        LEAD(amount) OVER w_recent AS next_amount,
        ROW_NUMBER() OVER w_recent AS seq_no
    FROM filtered
    WINDOW w_recent AS (PARTITION BY status, order_id ORDER BY amount_rounded, id)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount, id) = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC NULLS LAST, id) = 1
)
-- Multi-level aggregation
SELECT
    status,
    AVG(prev_amount_rounded) FILTER (WHERE method = 'cash') AS cash_avg_prev_amount_rounded,
    ROUND(SUM(next_amount_rounded) / NULLIF(COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END), 0), 2) AS next_amount_rounded_per_success,
    MAX(amount) AS max_amount
FROM latest
GROUP BY status
ORDER BY max_amount DESC
