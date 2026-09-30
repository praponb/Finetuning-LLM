WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        stock_qty + cost AS stock_qty_plus_cost,
        ROUND(cost * 0.85, 2) AS adjusted_cost,
        COALESCE(brand, 'NONE') AS brand_clean
    FROM products
    WHERE supplier_id IS NOT NULL
      AND brand IS DISTINCT FROM 'Globex'
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.store_id = 6
        )
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.revenue >= 3400
        )
),
not_flagged_products AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.units BETWEEN 13 AND 16
        )
),
valid_products AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SORT_ARRAY(tags) AS sorted_tags,
        COALESCE(brand, 'NONE') AS resolved_brand
    FROM not_flagged_products
    WHERE cost >= 590
)
-- Multi-level aggregation
SELECT
    category,
    CASE WHEN stock_qty_plus_cost >= 75 THEN 'LARGE' ELSE 'SMALL' END AS stock_qty_plus_cost_tier,
    COUNT(CASE WHEN brand = 'Initech' THEN 1 END) AS initech_count,
    APPROX_PERCENTILE(stock_qty_plus_cost, ARRAY(0.25, 0.5, 0.75)) AS stock_qty_plus_cost_quartiles,
    APPROX_PERCENTILE(stock_qty, 0.9) AS stock_qty_p90
FROM valid_products
GROUP BY GROUPING SETS ((category, CASE WHEN stock_qty_plus_cost >= 75 THEN 'LARGE' ELSE 'SMALL' END), (category))
HAVING initech_count > 1
ORDER BY category DESC NULLS LAST;
