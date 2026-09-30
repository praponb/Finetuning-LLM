WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LAST_DAY(delivered_date) AS delivered_date_month_end
    FROM shipments
    WHERE order_id IS NOT NULL
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.carrier,
        src.shipped_date,
        src.cost,
        src.delivered_date_month_end,
        p.discount,
        p.status AS order_status
    FROM cleaned src
    INNER JOIN orders p
        ON src.order_id = p.id
),
enriched_shipments AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.cost,
        s.discount,
        s.order_status,
        b.shipping_fee,
        b.discount AS order_discount
    FROM enriched s
    LEFT JOIN orders b
        ON s.order_id = b.id
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.cost,
        e.order_status,
        e.order_discount,
        d.amount,
        d.channel
    FROM enriched_shipments e
    INNER JOIN orders d
        ON e.order_id = d.id
)
SELECT
    id,
    cost,
    order_status,
    order_discount,
    channel
FROM with_dims
ORDER BY id, order_status;
