SELECT
    id,
    product_id,
    sale_date,
    revenue,
    CORR(units, revenue) OVER (PARTITION BY store_id) corr_units_revenue,
    LAST_VALUE(units, TRUE) OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_units,
    RANK() OVER (PARTITION BY store_id, product_id ORDER BY units DESC) units_rank
FROM store_sales;
