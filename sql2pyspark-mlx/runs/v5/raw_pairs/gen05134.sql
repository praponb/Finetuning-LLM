WITH valid AS (
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        INITCAP(TRIM(brand)) AS brand_title
    FROM products
    WHERE supplier_id IS NOT NULL
),
cleaned AS (
    SELECT
        *,
        SIZE(tags) AS tags_count,
        ARRAY_JOIN(tags, ', ') AS tags_text
    FROM valid
    WHERE brand IS DISTINCT FROM 'Acme'
      AND supplier_id IN (8, 12, 15)
),
enriched AS (
    SELECT
        *,
        AVG(tags_count) OVER (PARTITION BY category) AS category_avg_tags_count,
        CUME_DIST() OVER (PARTITION BY supplier_id ORDER BY cost DESC NULLS FIRST) AS cost_cume_dist,
        FIRST_VALUE(stock_qty, TRUE) OVER (PARTITION BY supplier_id ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_stock_qty
    FROM cleaned
    WHERE category <> 'Books'
),
eligible AS (
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.sale_date <= s.launch_date
        )
)
SELECT
    DATE_TRUNC('MONTH', launch_date) AS activity_month,
    brand,
    SUM(price) AS total_price,
    COUNT_IF(brand = 'Stark') AS stark_events,
    COUNT(CASE WHEN brand = 'Stark' THEN 1 END) AS stark_count,
    COUNT_IF(category = 'Sports') AS sports_events
FROM eligible
GROUP BY DATE_TRUNC('MONTH', launch_date), brand
HAVING stark_events > 1
ORDER BY stark_events DESC;
