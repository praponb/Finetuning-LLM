WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_TRUNC('month', last_restock) AS last_restock_period,
        ADD_MONTHS(last_restock, 12) AS last_restock_plus_months
    FROM inventory
    WHERE last_restock >= DATE '2023-04-15'
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(last_restock_period) OVER (PARTITION BY store_id ORDER BY last_restock_period, id) AS prev_last_restock_period
    FROM valid
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_last_restock_period IS NULL OR last_restock_period > prev_last_restock_period + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessions_inventory AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY store_id ORDER BY last_restock_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
valid_inventory AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        last_restock_plus_months,
        prev_last_restock_period,
        new_group_flag,
        island_id,
        ROUND(new_group_flag * 0.85, 2) AS adjusted_new_group_flag
    FROM sessions_inventory
    WHERE island_id <= 3
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY island_id, id) AS rn
    FROM valid_inventory
    QUALIFY rn = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY rn) AS row_num
        FROM ranked
    ) ranked
    WHERE row_num = 1
)
-- Ranking and gap features
SELECT
    id,
    last_restock,
    last_restock_plus_months,
    prev_last_restock_period,
    new_group_flag,
    adjusted_new_group_flag,
    AVG(adjusted_new_group_flag) OVER (PARTITION BY product_id ORDER BY island_id, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_adjusted_new_group_flag,
    COUNT(adjusted_new_group_flag) OVER (PARTITION BY product_id ORDER BY island_id, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_adjusted_new_group_flag,
    MIN(adjusted_new_group_flag) OVER (PARTITION BY product_id ORDER BY island_id, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_adjusted_new_group_flag
FROM deduped
WHERE island_id > 7
