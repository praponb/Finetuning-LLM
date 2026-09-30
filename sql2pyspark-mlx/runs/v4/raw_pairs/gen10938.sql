WITH cleaned AS (
    SELECT
        *,
        LEAST(revenue, units) AS least_revenue_units,
        DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_date_label
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-12-16')
)
SELECT
    *,
    CORR(units, revenue) OVER w_part AS corr_units_revenue,
    FIRST_VALUE(least_revenue_units) IGNORE NULLS OVER (PARTITION BY product_id, store_id ORDER BY revenue ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_least_revenue_units,
    LEAD(revenue, 1) OVER (PARTITION BY store_id ORDER BY least_revenue_units ASC, id ASC) AS next_revenue
FROM cleaned
WINDOW w_part AS (PARTITION BY store_id)
