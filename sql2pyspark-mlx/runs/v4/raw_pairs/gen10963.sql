WITH ranked_suppliers AS (
    SELECT
        id,
        country,
        rating,
        city,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS row_num
    FROM suppliers
)
SELECT *
FROM ranked_suppliers
WHERE row_num <= 3;
