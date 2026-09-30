WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost ASC, id ASC) AS rn
    FROM shipments
    QUALIFY rn = 1
),
ranked_shipments AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost DESC, id ASC) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        carrier,
        cost,
        ROUND(cost / SUM(cost) OVER (PARTITION BY order_id, status), 4) cost_share,
        FIRST_VALUE(carrier) IGNORE NULLS OVER (PARTITION BY status ORDER BY delivered_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_carrier
    FROM ranked_shipments
    WHERE rn > 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        carrier,
        cost,
        cost_share,
        first_known_carrier
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id, carrier ORDER BY cost_share ASC NULLS LAST) AS rn
        FROM metrics
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    carrier,
    cost,
    cost_share,
    first_known_carrier
FROM deduped
ORDER BY id DESC, carrier ASC;
