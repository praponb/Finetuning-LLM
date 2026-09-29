WITH base AS (
    SELECT *, DATE_FORMAT(sale_date, 'yyyy-MM-dd') AS sale_date_label
    FROM store_sales
    WHERE sale_date >= DATE '2023-04-01'
      AND units BETWEEN 29 AND 40
)
SELECT
    id,
    product_id,
    sale_date,
    units,
    revenue,
    sale_date_label,
    RANK() OVER w_recent AS revenue_rank,
    LEAD(units, 1) OVER w_hist AS next_units,
    ROUND(units / SUM(units) OVER (PARTITION BY product_id), 4) AS units_share,
    CORR(revenue, units) OVER w_ordered AS corr_revenue_units,
    FIRST_VALUE(revenue) IGNORE NULLS OVER w_running AS first_known_revenue
FROM base
WINDOW w_recent AS (PARTITION BY store_id ORDER BY revenue DESC NULLS LAST), w_hist AS (PARTITION BY product_id ORDER BY revenue, id), w_ordered AS (PARTITION BY store_id), w_running AS (PARTITION BY store_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
