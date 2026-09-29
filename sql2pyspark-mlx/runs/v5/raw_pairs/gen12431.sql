WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATE_TRUNC('MONTH', last_restock) AS last_restock_period
    FROM inventory
    WHERE last_restock >= DATE '2023-09-06'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY product_id, store_id ORDER BY quantity DESC, id ASC) AS quantity_bucket,
        NTILE(10) OVER (PARTITION BY store_id ORDER BY quantity DESC, id ASC) AS quantity_bucket_2,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY last_restock ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS quantity_avg_5
    FROM filtered
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(last_restock_period) OVER (PARTITION BY store_id ORDER BY last_restock_period ASC, id ASC) AS prev_last_restock_period
    FROM enriched
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_last_restock_period IS NULL OR last_restock_period > prev_last_restock_period + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessions_inventory AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        store_id,
        last_restock,
        last_restock_period,
        quantity_bucket_2,
        quantity_avg_5,
        SUM(is_new_burst) OVER (PARTITION BY store_id ORDER BY last_restock_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY island_id DESC) AS dedup_rank
        FROM sessions_inventory
    ) x
    WHERE dedup_rank = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT store_id, id
    FROM latest
    EXCEPT ALL
    SELECT store_id, id
    FROM inventory
    WHERE quantity <= 187
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN kept_keys k ON s.store_id = k.store_id AND s.id = k.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(quantity_avg_5) OVER w_running AS quantity_avg_5_count_30d,
        RANK() OVER (PARTITION BY store_id ORDER BY island_id DESC) AS island_id_rank,
        STDDEV_SAMP(quantity_avg_5) OVER (PARTITION BY store_id) AS store_id_stddev_samp_quantity_avg_5
    FROM kept_rows
    WINDOW w_running AS (PARTITION BY store_id ORDER BY CAST(last_restock_period AS BIGINT) ASC RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', last_restock) AS activity_month,
    COUNT(*) AS num_rows,
    MAX(last_restock_period) AS last_last_restock_period,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_windows
GROUP BY activity_month
HAVING num_rows > 1;
