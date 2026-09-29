SELECT product_id, unique_stores FROM (SELECT product_id, COUNT(DISTINCT units) AS unique_stores FROM store_sales GROUP BY product_id) AS agg WHERE unique_stores > 9 ORDER BY product_id
