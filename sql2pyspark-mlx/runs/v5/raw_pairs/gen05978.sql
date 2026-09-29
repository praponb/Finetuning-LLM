WITH valid AS (
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
        LOWER(category) AS category_lower
    FROM products
    WHERE YEAR(launch_date) = 2023
)
SELECT s.*
FROM valid s
WHERE NOT EXISTS (
        SELECT 1
        FROM store_sales c
        WHERE c.product_id = s.id
          AND c.units BETWEEN 33 AND 37
    );
