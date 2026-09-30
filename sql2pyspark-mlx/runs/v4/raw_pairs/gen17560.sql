WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        LEAST(units, revenue) least_units_revenue,
        ROUND(units * 100, 2) units_adjusted
    FROM store_sales
    WHERE units BETWEEN 26 AND 35
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(least_units_revenue) OVER (PARTITION BY store_id, product_id) store_id_count_least_units_revenue,
        SUM(least_units_revenue) OVER (PARTITION BY product_id ORDER BY units_adjusted, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_least_units_revenue,
        PERCENT_RANK() OVER (ORDER BY units_adjusted DESC NULLS LAST) units_adjusted_percent_rank
    FROM cleaned
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(store_id_count_least_units_revenue, TRUE) OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_store_id_count_least_units_revenue,
        AVG(units) OVER (PARTITION BY product_id ORDER BY least_units_revenue, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) units_avg_6,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY units_adjusted_percent_rank NULLS LAST) units_adjusted_percent_rank_cume_dist,
        AVG(units_adjusted) OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) units_adjusted_avg_2,
        CUME_DIST() OVER (ORDER BY running_least_units_revenue NULLS LAST) running_least_units_revenue_cume_dist
    FROM with_windows
),
enriched AS (
    -- Attach stores attributes
    SELECT
        e.*,
        b.city,
        b.store_name,
        b.sqft
    FROM scored e
    INNER JOIN stores b
        ON e.store_id = b.id
),
enriched_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(units_avg_6) OVER (PARTITION BY city, store_id ORDER BY units_adjusted_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_units_avg_6,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY store_id_count_least_units_revenue DESC NULLS LAST) store_id_count_least_units_revenue_dense_rank,
        NTILE(4) OVER (PARTITION BY store_id ORDER BY store_id_count_least_units_revenue, id) store_id_count_least_units_revenue_bucket
    FROM enriched
),
with_dims AS (
    -- Attach products attributes
    SELECT a.*, p.category
    FROM enriched_store_sales a
    INNER JOIN products p
        ON a.product_id = p.id
)
-- Ranking and gap features
SELECT
    id,
    store_id,
    product_id,
    sale_date,
    units,
    least_units_revenue,
    units_adjusted,
    store_id_count_least_units_revenue,
    running_least_units_revenue,
    units_adjusted_percent_rank,
    last_known_store_id_count_least_units_revenue,
    units_avg_6,
    units_adjusted_percent_rank_cume_dist,
    units_adjusted_avg_2,
    running_least_units_revenue_cume_dist,
    city,
    store_name,
    sqft,
    running_units_avg_6,
    store_id_count_least_units_revenue_dense_rank,
    store_id_count_least_units_revenue_bucket,
    category,
    CUME_DIST() OVER w_ordered store_id_count_least_units_revenue_cume_dist,
    SUM(least_units_revenue) OVER (PARTITION BY store_id) store_id_sum_least_units_revenue
FROM with_dims
WINDOW w_ordered AS (PARTITION BY city ORDER BY store_id_count_least_units_revenue DESC)
