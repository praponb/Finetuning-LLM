WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN cost >= 88 THEN 'High' WHEN cost >= 46 THEN 'Medium' ELSE 'Low' END AS cost_band,
        COALESCE(carrier, 'OTHER') AS carrier_clean
    FROM shipments
    WHERE carrier IS NOT NULL
      AND cost <= 85
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost ASC, id ASC) AS latest_rank
    FROM filtered
    QUALIFY latest_rank = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.status AS order_status,
        d.amount
    FROM latest e
    LEFT JOIN orders d
        ON e.order_id = d.id
    WHERE d.shipping_fee >= 26
),
enriched_shipments AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(cost) OVER (PARTITION BY carrier ORDER BY shipped_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_cost,
        COUNT(cost) OVER (PARTITION BY carrier ORDER BY shipped_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_cost,
        MIN(cost) OVER (PARTITION BY carrier ORDER BY shipped_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_cost
    FROM enriched
),
enriched_shipments_2 AS (
    -- Attach orders attributes
    SELECT src.*, p.discount
    FROM enriched_shipments src
    LEFT JOIN orders p
        ON src.order_id = p.id
)
SELECT
    id,
    carrier,
    shipped_date,
    cost_band,
    carrier_clean,
    order_status,
    amount,
    last7_max_cost,
    last7_min_cost
FROM enriched_shipments_2
ORDER BY amount ASC NULLS LAST, carrier DESC
