WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(units, 0) AS units_filled
    FROM store_sales
    WHERE units = 22
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(revenue) OVER w_hist AS running_revenue,
        STDDEV_SAMP(units_filled) OVER w_part AS product_id_stddev_samp_units_filled,
        ROUND(units / SUM(units) OVER (PARTITION BY product_id), 4) AS units_share
    FROM cleaned
    WINDOW w_hist AS (PARTITION BY store_id, product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY product_id, store_id)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        units_filled,
        product_id_stddev_samp_units_filled,
        units_share,
        SUM(revenue) OVER w_running AS running_sum_revenue,
        MIN(revenue) OVER w_running AS running_min_revenue,
        COUNT(revenue) OVER w_running AS running_count_revenue,
        LAG(units_filled, 2) OVER w_part AS prev_units_filled
    FROM scored
    WINDOW w_running AS (PARTITION BY product_id ORDER BY product_id_stddev_samp_units_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY store_id ORDER BY units_share, id)
),
scored_store_sales AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        sale_date,
        units,
        running_min_revenue,
        MIN(running_count_revenue) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_running_count_revenue,
        MAX(running_count_revenue) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_running_count_revenue,
        COUNT(running_count_revenue) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_running_count_revenue,
        SUM(running_count_revenue) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_running_count_revenue
    FROM metrics
),
metrics_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY units DESC NULLS FIRST) AS units_percent_rank,
        AVG(last4_min_running_count_revenue) OVER w_ordered AS last4_min_running_count_revenue_avg_5_prior,
        RANK() OVER (PARTITION BY product_id ORDER BY last4_sum_running_count_revenue) AS last4_sum_running_count_revenue_rank,
        AVG(last4_min_running_count_revenue) OVER w_ordered_2 AS last4_min_running_count_revenue_avg_3,
        LAG(last4_max_running_count_revenue, 1) OVER (PARTITION BY product_id ORDER BY units, id) AS prev_last4_max_running_count_revenue
    FROM scored_store_sales
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY running_min_revenue, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w_ordered_2 AS (PARTITION BY product_id ORDER BY last4_min_running_count_revenue, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    product_id,
    units,
    running_min_revenue,
    last4_min_running_count_revenue,
    last4_min_running_count_revenue_avg_5_prior,
    last4_min_running_count_revenue_avg_3,
    prev_last4_max_running_count_revenue
FROM metrics_store_sales
ORDER BY last4_min_running_count_revenue_avg_3;
