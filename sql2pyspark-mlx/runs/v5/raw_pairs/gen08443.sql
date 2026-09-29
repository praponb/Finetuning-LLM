WITH metrics AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        NTILE(3) OVER (PARTITION BY product_id, store_id ORDER BY revenue DESC, id) AS revenue_bucket,
        AVG(units) OVER (PARTITION BY store_id, product_id ORDER BY sale_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS units_avg_6,
        SUM(units) OVER (ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units
    FROM store_sales
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.sale_date,
    s.units,
    s.revenue_bucket,
    (SELECT MAX(x.revenue) FROM store_sales x WHERE x.store_id = s.store_id) AS max_store_sale_revenue
FROM metrics s
