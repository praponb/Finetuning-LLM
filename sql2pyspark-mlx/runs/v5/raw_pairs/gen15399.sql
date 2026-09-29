SELECT
    *,
    NTILE(4) OVER (PARTITION BY store_id ORDER BY revenue, id) AS revenue_bucket,
    AVG(units) OVER (ORDER BY sale_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS units_avg_2,
    CORR(units, revenue) OVER (PARTITION BY store_id) AS corr_units_revenue
FROM store_sales
WHERE product_id = 6;
