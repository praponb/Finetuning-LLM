SELECT *, ROUND(rating, 0) AS rating_rounded
FROM suppliers
WHERE country = 'SG' AND LENGTH(supplier_name) > 6
ORDER BY 1 DESC
