WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, units + revenue AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > DATE '2023-07-15'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue_per_unit,
        CASE WHEN revenue >= 3600 THEN 'High' WHEN revenue >= 1800 THEN 'Medium' ELSE 'Low' END AS revenue_tier
    FROM base
    WHERE revenue > 2000
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY revenue_per_unit DESC, id) AS row_num
    FROM cleaned
    QUALIFY row_num = 1
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.*,
        d.launch_date,
        d.category,
        d.cost
    FROM deduped src
    INNER JOIN products d
        ON src.product_id = d.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        row_num,
        launch_date,
        category,
        cost,
        LEAD(revenue_per_unit) OVER (PARTITION BY store_id, category ORDER BY sale_date, id) AS next_revenue_per_unit,
        LAG(cost) OVER (PARTITION BY store_id, category ORDER BY sale_date, id) AS prev_cost,
        ROW_NUMBER() OVER (PARTITION BY store_id, category ORDER BY sale_date, id) AS seq_no
    FROM with_dims
    WHERE units <= 11
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY row_num DESC, id) AS row_num_2
        FROM scored
    ) ranked
    WHERE row_num_2 = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(prev_cost) OVER w_grp AS running_count_prev_cost,
        MAX(prev_cost) OVER w_grp AS running_max_prev_cost,
        SUM(prev_cost) OVER w_grp AS running_sum_prev_cost
    FROM ranked
    WINDOW w_grp AS (PARTITION BY category, store_id ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    store_id,
    CASE WHEN running_max_prev_cost >= 610 THEN 'HIGH' ELSE 'LOW' END AS running_max_prev_cost_tier,
    COUNT(*) AS event_count,
    SUM(running_sum_prev_cost) AS total_running_sum_prev_cost
FROM metrics
GROUP BY ROLLUP (store_id, CASE WHEN running_max_prev_cost >= 610 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY event_count
