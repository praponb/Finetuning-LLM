WITH ranked_stores AS (
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY sqft DESC) AS row_num
    FROM stores
    WHERE opened_date >= '2025-06-01'
)
SELECT id, city, sqft
FROM ranked_stores
WHERE row_num = 1;
