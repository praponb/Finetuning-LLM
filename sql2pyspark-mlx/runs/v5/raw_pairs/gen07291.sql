SELECT revenue
FROM store_sales
WHERE revenue IS NOT NULL AND YEAR(sale_date) = 2023 AND units BETWEEN 22 AND 32
