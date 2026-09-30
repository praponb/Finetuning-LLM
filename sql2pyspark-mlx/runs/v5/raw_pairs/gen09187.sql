WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM inventory
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock, id) = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_quantity,
        MIN(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_quantity,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_quantity,
        SUM(quantity) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity
    FROM ranked
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        last_restock,
        last4_min_quantity,
        LAG(last4_min_quantity, 1) OVER (PARTITION BY product_id ORDER BY cumulative_quantity, id) AS prev_last4_min_quantity,
        AVG(cumulative_quantity) OVER (PARTITION BY store_id ORDER BY cumulative_quantity, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS cumulative_quantity_avg_6_prior,
        AVG(last4_min_quantity) OVER (ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last4_min_quantity_avg_6
    FROM with_windows
    WHERE last4_min_quantity >= 100
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.category,
        p.launch_date
    FROM scored src
    INNER JOIN products p
        ON src.product_id = p.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(last4_min_quantity_avg_6) OVER w_grp AS running_last4_min_quantity_avg_6,
        AVG(prev_last4_min_quantity) OVER (PARTITION BY product_id ORDER BY last4_min_quantity, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS prev_last4_min_quantity_avg_3_prior,
        RANK() OVER (PARTITION BY product_id ORDER BY prev_last4_min_quantity DESC) AS prev_last4_min_quantity_rank,
        MIN(last4_min_quantity_avg_6) OVER w_hist AS category_min_last4_min_quantity_avg_6,
        AVG(last4_min_quantity) OVER (PARTITION BY product_id, category ORDER BY cumulative_quantity_avg_6_prior, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS last4_min_quantity_avg_2_prior
    FROM with_dims
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY category)
),
joined AS (
    -- Attach products attributes
    SELECT
        a.*,
        p.launch_date AS product_launch_date,
        p.cost,
        p.stock_qty
    FROM enriched a
    INNER JOIN products p
        ON a.product_id = p.id
),
enriched_inventory AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(prev_last4_min_quantity_avg_3_prior) OVER (PARTITION BY product_id, category ORDER BY category_min_last4_min_quantity_avg_6, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS prev_last4_min_quantity_avg_3_prior_avg_2,
        LEAD(prev_last4_min_quantity_avg_3_prior, 1) OVER (PARTITION BY product_id ORDER BY last4_min_quantity_avg_6, id) AS next_prev_last4_min_quantity_avg_3_prior
    FROM joined
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN last4_min_quantity_avg_2_prior >= 251 THEN 'HIGH' ELSE 'LOW' END AS last4_min_quantity_avg_2_prior_tier,
    CASE WHEN cost >= 550 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_tier,
    category,
    PERCENTILE_APPROX(prev_last4_min_quantity_avg_3_prior_avg_2, 0.95) AS prev_last4_min_quantity_avg_3_prior_avg_2_p95,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list_2
FROM enriched_inventory
GROUP BY CASE WHEN last4_min_quantity_avg_2_prior >= 251 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN cost >= 550 THEN 'PREMIUM' ELSE 'STANDARD' END, category
HAVING prev_last4_min_quantity_avg_3_prior_avg_2_p95 >= 0
ORDER BY category DESC
