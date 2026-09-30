WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(carrier, ' ', status) carrier_status
    FROM shipments
    WHERE order_id IS NOT NULL
      AND carrier IN ('UPS', 'Kerry')
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.*,
        b.shipping_fee,
        b.status AS order_status
    FROM base s
    INNER JOIN orders b
        ON s.order_id = b.id
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.*,
        o.order_date,
        o.channel,
        o.amount
    FROM enriched src
    LEFT JOIN orders o
        ON src.order_id = o.id
)
-- Grouped metrics
SELECT
    channel,
    ROUND(AVG(cost), 2) AS avg_cost,
    ROUND(AVG(amount), 2) AS avg_amount
FROM joined
GROUP BY 1
ORDER BY channel DESC;
