WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(method, 'unassigned') AS method_filled
    FROM payments
    WHERE order_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        method_filled,
        ROW_NUMBER() OVER (ORDER BY paid_at DESC, id) AS seq_no,
        MAX(amount) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_max_30d,
        AVG(amount) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS amount_avg_6,
        LAST_VALUE(amount, TRUE) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount
    FROM valid
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY last_known_amount DESC, id) AS rn
        FROM with_windows
    ) ranked
    WHERE rn = 1
)
SELECT
    s.id,
    s.order_id,
    s.amount,
    s.paid_at,
    s.status,
    s.method_filled,
    s.seq_no,
    s.amount_max_30d,
    s.amount_avg_6,
    (SELECT SUM(s2.cost) FROM shipments s2 WHERE s2.order_id = s.order_id) AS sum_shipment_cost
FROM ranked s
ORDER BY seq_no NULLS FIRST, amount_max_30d
