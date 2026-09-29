WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        SUM(units) OVER w_running AS running_units,
        AVG(revenue) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS revenue_avg_3_prior,
        AVG(units) OVER w_running_2 AS units_avg_3
    FROM store_sales
    WINDOW w_running AS (PARTITION BY store_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running_2 AS (PARTITION BY product_id, store_id ORDER BY revenue, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        running_units,
        revenue_avg_3_prior,
        units_avg_3,
        FIRST_VALUE(running_units, TRUE) OVER (PARTITION BY product_id ORDER BY revenue_avg_3_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_units,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units, id) AS event_seq,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY units DESC NULLS LAST) AS units_dense_rank
    FROM with_windows
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        running_units,
        revenue_avg_3_prior,
        first_known_running_units,
        event_seq,
        units_dense_rank,
        ROUND(units_dense_rank * 100, 2) AS units_dense_rank_adjusted
    FROM scored
    WHERE sale_date >= DATE '2023-06-22'
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN units_dense_rank >= 8 THEN 'High' WHEN units_dense_rank >= 4 THEN 'Medium' ELSE 'Low' END AS units_dense_rank_tier
    FROM filtered
    WHERE units_dense_rank_adjusted >= 63
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        s.*,
        p2.sqft,
        p2.opened_date
    FROM valid s
    LEFT JOIN stores p2
        ON s.store_id = p2.id
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT store_id
    FROM with_dims
    INTERSECT ALL
    SELECT store_id
    FROM store_sales
    WHERE units > 38
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    JOIN remaining_keys k ON s.store_id = k.store_id
)
SELECT
    id,
    store_id,
    product_id,
    sale_date,
    running_units,
    event_seq,
    units_dense_rank,
    units_dense_rank_adjusted,
    units_dense_rank_tier
FROM verified
ORDER BY event_seq DESC
