WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(CURRENT_DATE(), paid_at) AS days_since_paid_at
    FROM payments
    WHERE paid_at >= DATE '2023-04-26'
      AND amount > 1490
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY amount DESC, id) AS row_num
        FROM filtered
    ) x
    WHERE row_num = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT status
    FROM ranked
    EXCEPT
    SELECT status
    FROM payments
    WHERE method != 'promptpay'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN kept_keys k ON s.status = k.status
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.method,
        e.amount,
        e.paid_at,
        e.row_num,
        p.shipping_fee,
        p.status AS order_status,
        p.discount
    FROM kept_rows e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
enriched AS (
    -- Attach orders attributes
    SELECT s.*, p.discount AS order_discount
    FROM with_dims s
    LEFT JOIN orders p
        ON s.order_id = p.id
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.amount,
    s.paid_at,
    s.order_status,
    s.discount,
    s.order_discount,
    (SELECT AVG(s2.cost) FROM shipments s2 WHERE s2.order_id = s.order_id) AS avg_shipment_cost
FROM enriched s
ORDER BY avg_shipment_cost DESC NULLS LAST;
