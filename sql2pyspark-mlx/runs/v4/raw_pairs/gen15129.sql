SELECT category, brand, avg_price
FROM (
    SELECT category, brand, ROUND(AVG(price), 2) AS avg_price
    FROM products
    WHERE UPPER(product_name) LIKE '%UNN%'
    GROUP BY category, brand
) AS x
WHERE avg_price > 44
