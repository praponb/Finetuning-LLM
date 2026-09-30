WITH ValidProducts AS (
    SELECT *, ROUND(price, 0) AS price_rounded
    FROM products
    WHERE supplier_id IN (5, 15, 17, 36) AND brand != 'Umbrella' AND ARRAY_CONTAINS(tags, 'premium')
),
products_stats AS (
    SELECT category, COUNT(*) AS num_products
    FROM ValidProducts
    GROUP BY category
)
SELECT *
FROM products_stats
