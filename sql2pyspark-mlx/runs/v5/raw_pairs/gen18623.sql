WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        LOWER(carrier) AS carrier_lower,
        ROUND(cost, 0) AS cost_rounded
    FROM shipments
    WHERE carrier IS NOT NULL
)
-- Keep one row per key (latest first)
SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost NULLS FIRST, id) AS latest_rank
FROM cleaned
QUALIFY latest_rank = 1;
