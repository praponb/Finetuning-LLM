SELECT
    id,
    store_id,
    product_id,
    sale_date,
    units,
    revenue,
    SUM(units) OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units,
    CORR(revenue, units) OVER w_ordered AS corr_revenue_units
FROM store_sales
WINDOW w_ordered AS (PARTITION BY store_id);
