WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        carrier,
        shipped_date,
        cost,
        DATE_TRUNC('week', delivered_date) AS delivered_date_period,
        ADD_MONTHS(delivered_date, 12) AS delivered_date_plus_months
    FROM shipments
    WHERE delivered_date >= DATE '2023-12-18'
      AND order_id IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(cost) OVER (PARTITION BY carrier) AS carrier_avg_cost,
        ROUND(cost / SUM(cost) OVER (PARTITION BY carrier), 4) AS cost_share
    FROM cleaned
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, carrier
    FROM enriched
    EXCEPT DISTINCT
    SELECT id, carrier
    FROM shipments
    WHERE status = 'DELIVERED'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.id = k.id AND s.carrier = k.carrier
)
SELECT
    carrier,
    cost,
    delivered_date_plus_months,
    carrier_avg_cost
FROM kept_rows
ORDER BY cost
