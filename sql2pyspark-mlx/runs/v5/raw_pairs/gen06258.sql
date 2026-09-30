WITH ranked_stores AS (
    SELECT *, RANK() OVER (PARTITION BY region ORDER BY opened_date DESC) AS pos
    FROM stores
)
SELECT *
FROM ranked_stores
WHERE pos <= 3
ORDER BY city
