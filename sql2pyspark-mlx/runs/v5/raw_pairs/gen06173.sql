WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        DATE_ADD(last_restock, 50) AS last_restock_plus_50d
    FROM inventory
    WHERE last_restock > TO_DATE('2023-08-30')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(quantity, 2) OVER (PARTITION BY store_id ORDER BY last_restock_plus_50d, id) AS prev_quantity,
        SUM(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity
    FROM base
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        quantity,
        last_restock,
        last_restock_plus_50d
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC NULLS FIRST, id) = 1
),
base_inventory AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_TRUNC('month', last_restock_plus_50d) AS last_restock_plus_50d_period
    FROM latest
    WHERE last_restock > DATE '2023-04-08'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        quantity,
        last_restock,
        LAST_VALUE(quantity) IGNORE NULLS OVER (ORDER BY last_restock_plus_50d_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
        NTILE(10) OVER (ORDER BY quantity, id) AS quantity_bucket
    FROM base_inventory
    WHERE last_restock_plus_50d <= '2025-05-01'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        quantity,
        last_restock,
        last_known_quantity,
        quantity_bucket,
        FLOOR(last_known_quantity) AS last_known_quantity_whole
    FROM with_windows
    WHERE last_restock >= TO_DATE('2023-06-15')
)
SELECT
    id,
    last_known_quantity,
    quantity_bucket,
    last_known_quantity_whole
FROM filtered
ORDER BY last_known_quantity_whole DESC
