WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(carrier, ', ', status) AS carrier_status
    FROM shipments
    WHERE delivered_date >= DATE '2023-06-27'
      AND carrier IS NOT NULL
),
enriched AS (
    -- Attach orders attributes
    SELECT s.*, d.discount
    FROM cleaned s
    JOIN orders d
        ON s.order_id = d.id
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched
    QUALIFY RANK() OVER (PARTITION BY status, carrier ORDER BY discount DESC NULLS LAST) = 1
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN cost >= 94 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_tier,
        SIZE(COLLECT_SET(carrier)) AS num_carriers,
        ROUND(AVG(cost), 2) AS avg_cost,
        ROUND(SUM(discount) / NULLIF(COUNT(CASE WHEN carrier = 'DHL' THEN 1 END), 0), 2) AS discount_per_dhl
    FROM latest
    GROUP BY CASE WHEN cost >= 94 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    cost_tier,
    avg_cost,
    discount_per_dhl
FROM stats
ORDER BY cost_tier DESC NULLS LAST
LIMIT 5;
