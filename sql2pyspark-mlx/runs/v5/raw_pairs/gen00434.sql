SELECT
    id,
    product_id,
    units,
    revenue,
    PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY revenue DESC) revenue_percent_rank,
    PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY units NULLS LAST) units_percent_rank,
    AVG(units) OVER (PARTITION BY store_id ORDER BY revenue, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) units_avg_5_prior,
    LEAD(revenue, 1) OVER (PARTITION BY product_id ORDER BY units, id) next_revenue,
    NTILE(5) OVER (PARTITION BY product_id ORDER BY units DESC, id) units_bucket
FROM store_sales
WHERE MONTH(sale_date) IN (3, 5, 7)
