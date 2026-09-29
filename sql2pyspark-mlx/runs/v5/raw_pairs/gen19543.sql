WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(amount * 0.9, 2) AS adjusted_amount
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-09-09 03:00:00'
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        o.status AS order_status,
        o.channel
    FROM valid e
    LEFT JOIN orders o
        ON e.order_id = o.id
    WHERE o.amount <= 1300
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        amount,
        paid_at,
        status,
        adjusted_amount
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY adjusted_amount DESC NULLS FIRST, id) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS amount_avg_2,
        COUNT(adjusted_amount) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS adjusted_amount_count_90d,
        MAX(adjusted_amount) OVER (PARTITION BY status) AS status_max_adjusted_amount,
        MAX(amount) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_max_30d
    FROM ranked
)
SELECT
    id,
    amount,
    paid_at,
    status,
    adjusted_amount,
    amount_avg_2,
    adjusted_amount_count_90d,
    amount_max_30d,
    CASE WHEN status RLIKE '^(REFUNDED|SUCCESS)$' THEN 'OTHER_ZONE' WHEN status RLIKE '^(FAILED)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS status_zone
FROM with_windows
ORDER BY adjusted_amount;
