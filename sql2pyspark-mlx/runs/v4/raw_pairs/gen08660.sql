WITH Ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price, id) row_num
    FROM products
)
SELECT *
FROM Ranked
WHERE row_num <= 5
ORDER BY supplier_id DESC;
