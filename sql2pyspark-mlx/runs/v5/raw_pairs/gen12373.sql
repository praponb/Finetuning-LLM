WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        DATE_TRUNC('year', last_restock) AS last_restock_period
    FROM inventory
    WHERE last_restock >= DATE '2023-04-26'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(quantity, TRUE) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
        LAST_VALUE(quantity, TRUE) OVER (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity_2
    FROM filtered
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(last_known_quantity) AS last_known_quantity_whole
    FROM with_windows
    WHERE last_restock >= DATE '2023-08-22'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_id,
        last_known_quantity,
        last_known_quantity_2
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_known_quantity DESC NULLS FIRST, id) = 1
)
-- Window metrics over several different partitions
SELECT
    id,
    store_id,
    SUM(last_known_quantity_2) OVER (PARTITION BY store_id ORDER BY last_known_quantity_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_last_known_quantity_2,
    STDDEV_SAMP(last_known_quantity_2) OVER (PARTITION BY store_id) AS store_id_stddev_samp_last_known_quantity_2,
    PERCENT_RANK() OVER (ORDER BY last_known_quantity_2 DESC NULLS FIRST) AS last_known_quantity_2_percent_rank
FROM latest
