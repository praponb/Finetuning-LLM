WITH cleaned AS (
    SELECT
        *,
        CONCAT_WS(',', region, city) AS region_city_key,
        COALESCE(region, 'DIRECT') AS region_clean
    FROM stores
    WHERE region IS NOT NULL
),
latest_match AS (
    SELECT s.*, x.inventory_last_restock
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest AS (
    SELECT
        s.id,
        s.region,
        s.opened_date,
        s.sqft,
        s.inventory_last_restock,
        latest.store_sale_revenue,
        latest.store_sale_units
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue, c.units AS store_sale_units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    SELECT
        id,
        region,
        sqft,
        store_sale_revenue,
        store_sale_units
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY store_sale_units DESC NULLS LAST, id) AS row_num
        FROM with_latest
    ) t
    WHERE row_num = 1
)
SELECT
    CASE WHEN store_sale_revenue >= 3200 THEN 'HIGH' ELSE 'LOW' END AS store_sale_revenue_tier,
    COUNT(*) FILTER (WHERE region = 'South') AS south_rows,
    COUNT(CASE WHEN region = 'East' THEN 1 END) AS east_count,
    SUM(CASE WHEN region = 'West' THEN store_sale_units ELSE 0 END) AS west_store_sale_units
FROM deduped
GROUP BY 1
HAVING east_count < 1000
ORDER BY south_rows;
