WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        status,
        LAG(cost) OVER w_part AS prev_cost,
        ROW_NUMBER() OVER w_part AS seq_no,
        LAST_VALUE(cost) IGNORE NULLS OVER (PARTITION BY status, carrier ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost
    FROM shipments
    WHERE shipped_date >= '2024-01-01'
    WINDOW w_part AS (PARTITION BY order_id ORDER BY delivered_date, id)
)
SELECT
    id,
    carrier,
    delivered_date,
    status,
    prev_cost,
    seq_no
FROM with_windows
ORDER BY delivered_date NULLS LAST, status DESC
