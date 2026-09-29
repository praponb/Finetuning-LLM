WITH supplier_stats AS (
    SELECT supplier_id, COUNT(cost) AS cost_count, SUM(CASE WHEN brand = 'Globex' THEN price ELSE 0 END) AS globex_price
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.supplier_name,
    s.rating,
    ss.cost_count,
    ss.globex_price
FROM suppliers s
INNER JOIN supplier_stats ss ON s.id = ss.supplier_id
WHERE ss.globex_price >= 49
ORDER BY cost_count DESC
