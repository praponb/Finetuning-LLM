WITH recent_products AS (
    SELECT *, CAST(price AS INT) AS price_int
    FROM products
    WHERE price >= 1170 AND ARRAY_CONTAINS(tags, 'bestseller') AND product_name IS NULL
),
launch_month_summary AS (
    SELECT DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month, category, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values
    FROM recent_products
    GROUP BY DATE_FORMAT(launch_date, 'yyyy-MM'), category
)
SELECT launch_month, category, category_values
FROM launch_month_summary
