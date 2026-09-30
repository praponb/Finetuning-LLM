SELECT YEAR(sale_date) AS sale_year, store_id, SUM(units) AS total_units, MAX(sale_date) AS last_sale_date FROM store_sales WHERE units BETWEEN 18 AND 20 GROUP BY ROLLUP(YEAR(sale_date), store_id);
