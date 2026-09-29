SELECT product_id, max_revenue FROM (SELECT product_id, MAX(revenue) AS max_revenue FROM store_sales GROUP BY product_id) AS agg WHERE max_revenue >= 1300;
