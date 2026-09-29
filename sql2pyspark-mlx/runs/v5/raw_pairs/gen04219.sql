WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY order_date, id) AS row_num
    FROM orders
    WHERE order_date <= '2025-05-01'
)
SELECT *
FROM ranked
WHERE row_num <= 3
ORDER BY order_date;
