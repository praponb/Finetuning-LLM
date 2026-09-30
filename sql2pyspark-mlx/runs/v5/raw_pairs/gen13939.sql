WITH kept_keys AS (
    -- Set difference on keys
    SELECT order_id
    FROM shipments
    EXCEPT ALL
    SELECT order_id
    FROM shipments
    WHERE cost BETWEEN 49 AND 92
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM shipments s
    JOIN kept_keys k ON s.order_id = k.order_id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        shipped_date,
        DATE_ADD(delivered_date, 57) AS delivered_date_plus_57d,
        COALESCE(cost, 0) AS cost_filled
    FROM reconciled
    WHERE carrier IN ('FedEx', 'DHL')
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT order_id, id
    FROM base
    INTERSECT
    SELECT order_id, id
    FROM shipments
    WHERE order_id IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.order_id = k.order_id AND s.id = k.id
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        d.order_date,
        d.shipping_fee,
        d.status
    FROM verified src
    JOIN orders d
        ON src.order_id = d.id
),
with_dims_shipments AS (
    -- Attach orders attributes
    SELECT a.*, d.discount
    FROM with_dims a
    LEFT JOIN orders d
        ON a.order_id = d.id
)
SELECT
    s.id,
    s.order_id,
    s.shipping_fee,
    s.discount,
    (SELECT AVG(p2.amount) FROM payments p2 WHERE p2.order_id = s.order_id) AS avg_payment_amount
FROM with_dims_shipments s
ORDER BY id, avg_payment_amount
