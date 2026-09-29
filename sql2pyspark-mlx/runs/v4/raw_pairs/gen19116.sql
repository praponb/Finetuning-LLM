WITH ranked_stores AS (
    SELECT
        id,
        city,
        opened_date,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) AS pos
    FROM stores
)
SELECT id, city, opened_date
FROM ranked_stores
WHERE pos <= 3
ORDER BY city DESC
