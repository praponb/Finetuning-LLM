WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, revenue - units revenue_per_unit
    FROM store_sales
    WHERE sale_date > DATE '2023-04-29'
      AND revenue <= 2300
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        units,
        revenue,
        LAST_VALUE(units) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY revenue_per_unit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_units,
        LAST_VALUE(revenue_per_unit) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_revenue_per_unit,
        AVG(units) OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) units_avg_6_prior,
        LEAD(revenue, 1) OVER (PARTITION BY product_id ORDER BY sale_date, id) next_revenue,
        RANK() OVER (ORDER BY units DESC) units_rank
    FROM valid
    WHERE product_id IN (1, 5, 8, 10)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        revenue,
        last_known_units,
        units_rank,
        SUM(revenue) OVER (ORDER BY next_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_revenue,
        DENSE_RANK() OVER w_running next_revenue_dense_rank
    FROM metrics
    WINDOW w_running AS (ORDER BY next_revenue DESC NULLS FIRST)
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_windows
    EXCEPT DISTINCT
    SELECT id
    FROM store_sales
    WHERE store_id IN (1, 3, 4, 6)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.id = k.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        revenue,
        last_known_units,
        units_rank,
        next_revenue_dense_rank,
        CAST(running_revenue AS DOUBLE) running_revenue_num
    FROM verified
    WHERE units_rank > 6
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        revenue,
        last_known_units,
        units_rank,
        next_revenue_dense_rank,
        running_revenue_num,
        ROUND(revenue * 1.15, 2) adjusted_revenue,
        ROUND(revenue / last_known_units, 2) revenue_per_last_known_units
    FROM base
    WHERE last_known_units != 12
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        revenue,
        last_known_units,
        units_rank,
        running_revenue_num,
        DENSE_RANK() OVER (ORDER BY running_revenue_num DESC NULLS FIRST) running_revenue_num_dense_rank,
        CUME_DIST() OVER (ORDER BY last_known_units) last_known_units_cume_dist
    FROM cleaned
)
SELECT
    id,
    revenue,
    last_known_units,
    units_rank,
    running_revenue_num,
    running_revenue_num_dense_rank,
    last_known_units_cume_dist
FROM scored
ORDER BY units_rank DESC, revenue DESC;
