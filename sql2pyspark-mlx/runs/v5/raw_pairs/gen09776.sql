WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        TRIM(city) AS clean_city
    FROM stores
    WHERE opened_date > TO_DATE('2023-10-07')
      AND region IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.product_id = 8
        )
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.store_sale_units
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.units AS store_sale_units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.last_restock,
        latest.inventory_quantity
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.last_restock, c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    DATE_TRUNC('MONTH', last_restock) AS activity_month,
    CASE WHEN sqft >= 13105 THEN 'HIGH' ELSE 'LOW' END AS sqft_tier,
    SIZE(COLLECT_SET(region)) AS num_regions,
    APPROX_PERCENTILE(sqft, 0.9) AS sqft_p90,
    ROUND(AVG(sqft), 2) AS avg_sqft
FROM with_latest
GROUP BY quarter_start, activity_month, sqft_tier
HAVING ROUND(AVG(sqft), 2) > 0
ORDER BY quarter_start NULLS LAST;
