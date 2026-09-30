WITH with_windows AS (
    SELECT
        *,
        MIN(revenue) OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_revenue,
        MAX(revenue) OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_revenue,
        SUM(revenue) OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_revenue,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY revenue DESC NULLS FIRST) AS revenue_dense_rank
    FROM store_sales
),
metrics AS (
    SELECT
        *,
        LEAD(running_max_revenue, 2) OVER (ORDER BY running_min_revenue, id) AS next_running_max_revenue,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY store_id), 4) AS revenue_share,
        CORR(units, revenue) OVER (PARTITION BY store_id) AS corr_units_revenue
    FROM with_windows
),
base AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        running_min_revenue,
        running_sum_revenue,
        next_running_max_revenue,
        revenue_share,
        corr_units_revenue,
        ROUND(running_max_revenue, 2) AS running_max_revenue_rounded
    FROM metrics
    WHERE sale_date > DATE '2023-02-21'
),
with_windows_store_sales AS (
    SELECT
        *,
        SUM(corr_units_revenue) OVER (ORDER BY running_sum_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_corr_units_revenue,
        SUM(corr_units_revenue) OVER (ORDER BY running_min_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_corr_units_revenue_2,
        SUM(revenue_share) OVER (PARTITION BY store_id, product_id ORDER BY running_sum_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue_share
    FROM base
),
metrics_store_sales AS (
    SELECT
        *,
        LAG(running_sum_revenue, 1) OVER w_hist AS prev_running_sum_revenue,
        LEAD(corr_units_revenue, 2) OVER (ORDER BY corr_units_revenue, id) AS next_corr_units_revenue
    FROM with_windows_store_sales
    WINDOW w_hist AS (PARTITION BY product_id ORDER BY running_corr_units_revenue_2, id)
),
with_dims AS (
    SELECT
        src.*,
        p.region,
        p.city
    FROM metrics_store_sales src
    LEFT JOIN stores p
        ON src.store_id = p.id
    WHERE p.store_name LIKE '%Siam%'
)
SELECT *
FROM (
    SELECT
        product_id,
        city,
        cumulative_revenue_share
    FROM with_dims
) src
PIVOT (
    AVG(cumulative_revenue_share) AS amount,
    COUNT(1) AS item_count
    FOR city IN ('London' AS london, 'Berlin' AS berlin)
)
