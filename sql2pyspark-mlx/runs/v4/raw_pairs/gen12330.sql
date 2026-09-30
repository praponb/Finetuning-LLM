SELECT * FROM (SELECT product_id, COUNT(units) AS units_count, MAX(sale_date) AS last_sale_date FROM store_sales WHERE revenue >= 2400 GROUP BY product_id) AS t WHERE units_count > 8;
