SELECT YEAR(sale_date) sale_year, product_id, MIN(units) min_units
FROM store_sales
WHERE revenue BETWEEN 2100 AND 2800
GROUP BY YEAR(sale_date), product_id
HAVING min_units < 27
