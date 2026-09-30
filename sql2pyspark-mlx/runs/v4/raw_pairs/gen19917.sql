WITH base AS (
    SELECT
        *,
        CONCAT(category, ' ', brand) AS category_brand,
        SUBSTRING(product_name, 1, 4) AS product_name_prefix
    FROM products
    WHERE launch_date > '2025-09-06'
)
SELECT s.*
FROM base s
WHERE EXISTS (
        SELECT 1
        FROM order_items ch
        WHERE ch.product_id = s.id
          AND ch.order_id IN (27, 39)
    )
