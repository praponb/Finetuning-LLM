SELECT QUARTER(sale_date) AS sale_quarter, COUNT(DISTINCT revenue) AS unique_stores FROM store_sales WHERE units IS NOT NULL AND YEAR(sale_date) = 2024 AND units <= 38 GROUP BY sale_quarter
