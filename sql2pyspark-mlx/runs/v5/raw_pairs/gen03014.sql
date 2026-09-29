SELECT product_id, store_id, PERCENTILE_APPROX(revenue, 0.5) AS revenue_p50, COUNT(*) AS num_rows, APPROX_PERCENTILE(units, 0.95) AS units_p95 FROM store_sales GROUP BY product_id, store_id
