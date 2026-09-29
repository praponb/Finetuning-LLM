WITH top_suppliers AS (
    SELECT
        id,
        city,
        rating,
        supplier_name,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) rn
    FROM suppliers
)
SELECT *
FROM top_suppliers
WHERE rn <= 2
ORDER BY city;
