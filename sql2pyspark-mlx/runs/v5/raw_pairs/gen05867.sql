SELECT YEAR(sale_date) AS sale_year, MAX(revenue) AS max_revenue FROM store_sales GROUP BY YEAR(sale_date) HAVING MAX(revenue) >= 1300 ORDER BY sale_year
