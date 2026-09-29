SELECT
    id,
    sale_date,
    revenue,
    units,
    SUM(revenue) OVER (ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_revenue,
    RANK() OVER (ORDER BY units DESC) units_rank
FROM store_sales
ORDER BY 1 DESC;
