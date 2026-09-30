WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN cost >= 90 THEN 'High' WHEN cost >= 47 THEN 'Medium' ELSE 'Low' END AS cost_level,
        COALESCE(carrier, 'DIRECT') AS carrier_filled
    FROM shipments
    WHERE order_id IS NOT NULL
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        p.shipping_fee,
        p.status AS order_status
    FROM cleaned e
    INNER JOIN orders p
        ON e.order_id = p.id
),
enriched_shipments AS (
    -- Attach orders attributes
    SELECT src.*, o.channel
    FROM enriched src
    LEFT JOIN orders o
        ON src.order_id = o.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_TRUNC('year', shipped_date) AS shipped_date_period,
        shipping_fee * cost AS shipping_fee_times_cost
    FROM enriched_shipments
    WHERE shipped_date >= DATE '2023-04-30'
      AND carrier IS NOT NULL
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN shipping_fee_times_cost >= 71 THEN 'PREMIUM' ELSE 'STANDARD' END AS shipping_fee_times_cost_tier,
        AVG(shipping_fee) AS avg_shipping_fee,
        PERCENTILE_APPROX(cost, 0.95) AS cost_p95,
        SIZE(COLLECT_SET(channel)) AS num_channels,
        COUNT(*) FILTER (WHERE channel = 'partner') AS partner_rows,
        (UNIX_TIMESTAMP(MAX(shipped_date_period)) - UNIX_TIMESTAMP(MIN(shipped_date_period))) / 60 AS span_minutes
    FROM base
    GROUP BY shipping_fee_times_cost_tier
    HAVING num_channels >= 1
)
SELECT
    shipping_fee_times_cost_tier,
    avg_shipping_fee,
    cost_p95,
    num_channels,
    span_minutes
FROM grouped
ORDER BY span_minutes DESC NULLS FIRST;
