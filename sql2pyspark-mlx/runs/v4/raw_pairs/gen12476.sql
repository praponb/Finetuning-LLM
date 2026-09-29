WITH ProductStats AS (
    SELECT product_id, ROUND(STDDEV(units), 2) AS stddev_units
    FROM store_sales
    GROUP BY product_id
)
SELECT p.brand, p.price, P.stddev_units
FROM products p
JOIN ProductStats P ON p.id = P.product_id;
