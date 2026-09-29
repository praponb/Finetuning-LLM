WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(brand, ', ', product_name) AS brand_product_name
    FROM products
    WHERE supplier_id IS NOT NULL
      AND LOWER(TRIM(product_name)) = 'noise-cancel headphones'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.units != 23
        )
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        a.*,
        p.country,
        p.supplier_name
    FROM eligible a
    LEFT JOIN suppliers p
        ON a.supplier_id = p.id
),
with_dims_products AS (
    -- Attach suppliers attributes
    SELECT
        a.*,
        p.rating,
        p.supplier_name AS supplier_supplier_name,
        p.city
    FROM with_dims a
    LEFT JOIN suppliers p
        ON a.supplier_id = p.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        brand_product_name,
        country,
        supplier_name,
        rating,
        supplier_supplier_name,
        city,
        DENSE_RANK() OVER (PARTITION BY city, country ORDER BY stock_qty DESC NULLS LAST) AS stock_qty_dense_rank,
        ROUND(price / SUM(price) OVER (PARTITION BY country), 4) AS price_share,
        AVG(rating) OVER (PARTITION BY category ORDER BY rating, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rating_avg_5,
        SUM(price) OVER (PARTITION BY brand, category ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_price,
        NTILE(4) OVER (PARTITION BY country ORDER BY rating, id) AS rating_bucket
    FROM with_dims_products
)
-- Grouped metrics
SELECT
    CASE WHEN cost >= 770 THEN 'LARGE' ELSE 'SMALL' END AS cost_tier,
    DATE_TRUNC('YEAR', launch_date) AS year_start,
    MIN(rating_bucket) AS min_rating_bucket,
    ROUND(AVG(stock_qty), 2) AS avg_stock_qty,
    COUNT(*) FILTER (WHERE city = 'London') AS london_rows,
    COUNT(*) AS row_count,
    RANK() OVER (PARTITION BY CASE WHEN cost >= 770 THEN 'LARGE' ELSE 'SMALL' END ORDER BY COUNT(*) DESC) AS row_count_rank
FROM scored
GROUP BY CASE WHEN cost >= 770 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('YEAR', launch_date)
ORDER BY avg_stock_qty DESC NULLS LAST
