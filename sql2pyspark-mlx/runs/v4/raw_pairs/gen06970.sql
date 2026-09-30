WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LAST_DAY(sale_date) sale_date_month_end
    FROM store_sales
    WHERE store_id = 4
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        sale_date_month_end,
        FIRST_VALUE(units, TRUE) OVER (ORDER BY sale_date_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_units,
        AVG(revenue) OVER (PARTITION BY product_id ORDER BY sale_date_month_end, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) revenue_avg_5_prior,
        LEAD(revenue, 1) OVER (ORDER BY sale_date_month_end, id) next_revenue
    FROM base
),
with_windows_store_sales AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(revenue_avg_5_prior, 1) OVER (ORDER BY sale_date_month_end, id) prev_revenue_avg_5_prior,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id), 4) units_share,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY units DESC NULLS LAST) units_percent_rank
    FROM with_windows
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_windows_store_sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY units_percent_rank DESC NULLS FIRST, id) = 1
)
SELECT
    s.id,
    s.product_id,
    s.sale_date,
    s.units,
    s.revenue_avg_5_prior,
    s.next_revenue,
    s.prev_revenue_avg_5_prior,
    (SELECT AVG(t2.quantity) FROM inventory t2 WHERE t2.product_id = s.product_id) AS avg_inventory_quantity
FROM ranked s
