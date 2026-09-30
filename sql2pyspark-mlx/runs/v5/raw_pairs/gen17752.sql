WITH with_latest AS (
    SELECT
        s.*,
        top1.inventory_last_restock,
        top1.quantity
    FROM stores s
    JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock <= s.opened_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    CASE WHEN quantity >= 202 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    city,
    AVG(sqft) FILTER (WHERE region = 'West') AS west_avg_sqft,
    SUM(CASE WHEN region = 'East' THEN quantity ELSE 0 END) AS east_quantity,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    PERCENTILE_APPROX(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    SUM(CASE WHEN region = 'East' THEN sqft ELSE 0 END) AS east_sqft
FROM with_latest
GROUP BY CUBE (CASE WHEN quantity >= 202 THEN 'LARGE' ELSE 'SMALL' END, city)
