SELECT stores.city, ROUND(AVG(store_sales.units), 2) AS avg_units
FROM store_sales
RIGHT OUTER JOIN products ON store_sales.product_id = products.id
INNER JOIN stores ON store_sales.store_id = stores.id
WHERE products.price >= 950
GROUP BY stores.city
ORDER BY city ASC
