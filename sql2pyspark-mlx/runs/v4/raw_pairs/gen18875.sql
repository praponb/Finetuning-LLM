WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        ROUND(stock_qty, 2) AS stock_qty_rounded,
        COALESCE(brand, 'DIRECT') AS brand_clean
    FROM products
    WHERE launch_date > TO_DATE('2023-05-19')
      AND supplier_id IS NOT NULL
      AND MONTH(launch_date) IN (1, 5, 11)
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        b.country,
        b.supplier_name,
        b.rating
    FROM cleaned e
    LEFT JOIN suppliers b
        ON e.supplier_id = b.id
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        s.country AS supplier_country,
        s.supplier_name AS supplier_supplier_name
    FROM joined e
    LEFT JOIN suppliers s
        ON e.supplier_id = s.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(cost) OVER (PARTITION BY supplier_id, supplier_country ORDER BY cost, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS cost_avg_3_prior,
        LAG(rating, 1) OVER (PARTITION BY supplier_country ORDER BY launch_date, id) AS prev_rating,
        AVG(cost) OVER (PARTITION BY supplier_id ORDER BY stock_qty_rounded, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS cost_avg_5,
        FIRST_VALUE(stock_qty_rounded) IGNORE NULLS OVER (PARTITION BY country, supplier_id ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_stock_qty_rounded
    FROM enriched
)
-- Grouped metrics
SELECT
    supplier_id,
    CASE WHEN rating >= 4.1 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    APPROX_PERCENTILE(rating, ARRAY(0.25, 0.5, 0.75)) AS rating_quartiles,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    COUNT(*) FILTER (WHERE supplier_country = 'SG') AS sg_rows
FROM metrics
GROUP BY 1, 2
ORDER BY sg_rows DESC;
