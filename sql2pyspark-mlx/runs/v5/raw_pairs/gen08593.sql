WITH ranked_suppliers AS (
    SELECT *, RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS rn
    FROM suppliers
    WHERE city = 'Tokyo'
)
SELECT id, country, rating
FROM ranked_suppliers
WHERE rn = 1
