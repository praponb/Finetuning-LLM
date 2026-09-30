WITH enriched AS (
    SELECT a.*, p.product_name
    FROM store_sales a
    LEFT JOIN products p
        ON a.product_id = p.id
),
with_windows AS (
    SELECT
        *,
        SUM(revenue) OVER (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id), 4) AS units_share
    FROM enriched
),
latest AS (
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY running_revenue NULLS FIRST, id) = 1
),
scored AS (
    SELECT
        *,
        AVG(units_share) OVER (PARTITION BY store_id, product_id ORDER BY revenue, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS units_share_avg_3,
        SUM(revenue) OVER w_running AS running_revenue_2,
        FIRST_VALUE(revenue) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_revenue,
        AVG(units) OVER (PARTITION BY product_id) AS product_id_avg_units,
        SUM(running_revenue) OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_revenue
    FROM latest
    WINDOW w_running AS (PARTITION BY product_id, store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        product_name,
        running_revenue,
        units_share,
        units_share_avg_3,
        first_known_revenue,
        product_id_avg_units,
        cumulative_running_revenue,
        CASE WHEN units_share_avg_3 >= 1 THEN 'High' WHEN units_share_avg_3 >= 0 THEN 'Medium' ELSE 'Low' END AS units_share_avg_3_band
    FROM scored
    WHERE cumulative_running_revenue >= 57969
),
metrics AS (
    SELECT
        *,
        LAG(units_share, 1) OVER (PARTITION BY product_id, store_id ORDER BY units_share_avg_3, id) AS prev_units_share,
        CORR(units, revenue) OVER (PARTITION BY product_id) AS corr_units_revenue,
        LEAD(revenue, 1) OVER (PARTITION BY product_id ORDER BY running_revenue, id) AS next_revenue
    FROM cleaned
),
cleaned_store_sales AS (
    SELECT
        id,
        store_id,
        sale_date,
        units,
        revenue,
        product_name,
        running_revenue,
        units_share_avg_3,
        first_known_revenue,
        product_id_avg_units,
        units_share_avg_3_band,
        prev_units_share,
        corr_units_revenue,
        COALESCE(next_revenue, 0) AS next_revenue_filled,
        LENGTH(product_name) AS product_name_length
    FROM metrics
    WHERE sale_date > DATE '2023-07-30'
)
SELECT
    id,
    store_id,
    sale_date,
    revenue,
    product_name,
    units_share_avg_3,
    first_known_revenue,
    product_id_avg_units,
    prev_units_share
FROM cleaned_store_sales
