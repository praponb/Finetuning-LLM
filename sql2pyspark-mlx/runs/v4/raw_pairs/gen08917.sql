SELECT *
FROM (
    SELECT product_id, ROUND(AVG(units), 2) AS avg_units
    FROM store_sales
    WHERE revenue < 3000
    GROUP BY product_id
) AS sub
WHERE avg_units < 49
ORDER BY avg_units
