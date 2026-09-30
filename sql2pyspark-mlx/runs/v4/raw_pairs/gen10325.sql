WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        status,
        COALESCE(cost, 0) AS cost_filled
    FROM shipments
    WHERE carrier IS NOT NULL
      AND cost > 58
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.*,
        d.discount,
        d.order_date,
        d.amount
    FROM valid src
    LEFT JOIN orders d
        ON src.order_id = d.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, cost_filled - discount AS cost_filled_minus_discount
    FROM joined
    WHERE delivered_date > TO_DATE('2023-04-21')
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM cleaned
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY cost) = 1
),
valid_shipments AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        shipped_date,
        cost,
        status,
        cost_filled,
        discount,
        order_date,
        amount,
        cost_filled_minus_discount,
        ROUND(discount / amount, 2) AS discount_per_amount
    FROM latest
    WHERE order_id IS NOT NULL
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', shipped_date) AS quarter_start,
    CASE WHEN discount >= 0.35 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_tier,
    ROUND(SUM(discount_per_amount) / NULLIF(COUNT(CASE WHEN status = 'LOST' THEN 1 END), 0), 2) AS discount_per_amount_per_lost,
    COUNT(CASE WHEN status = 'IN_TRANSIT' THEN 1 END) AS in_transit_count,
    GROUPING(DATE_TRUNC('QUARTER', shipped_date)) AS grouping_id
FROM valid_shipments
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', shipped_date), CASE WHEN discount >= 0.35 THEN 'PREMIUM' ELSE 'STANDARD' END), (DATE_TRUNC('QUARTER', shipped_date)));
