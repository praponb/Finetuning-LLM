WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(stock_qty AS STRING) AS stock_qty_str
    FROM products
    WHERE supplier_id IS NOT NULL
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        a.id,
        a.brand,
        a.cost,
        a.supplier_id,
        b.city
    FROM valid a
    INNER JOIN suppliers b
        ON a.supplier_id = b.id
),
valid_products AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        brand,
        cost,
        supplier_id,
        city,
        SUBSTRING(city, 1, 4) AS city_prefix,
        COALESCE(brand, 'N/A') AS brand_filled
    FROM enriched
    WHERE city <> 'Tokyo'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        brand,
        supplier_id,
        city,
        AVG(cost) OVER (PARTITION BY brand, supplier_id ORDER BY cost, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cost_avg_6,
        AVG(cost) OVER (ORDER BY cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS cost_avg_3
    FROM valid_products
)
-- Grouped metrics
SELECT
    CASE WHEN cost_avg_3 >= 686 THEN 'LARGE' ELSE 'SMALL' END AS cost_avg_3_tier,
    brand,
    supplier_id,
    SUM(CASE WHEN brand = 'Globex' THEN cost_avg_6 ELSE 0 END) AS globex_cost_avg_6,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(CASE WHEN brand = 'Globex' THEN cost_avg_3 ELSE 0 END) AS globex_cost_avg_3,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN cost_avg_3 >= 686 THEN 'LARGE' ELSE 'SMALL' END ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_dense_rank
FROM with_windows
GROUP BY 1, 2, 3
