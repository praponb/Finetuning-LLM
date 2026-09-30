WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM inventory
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_restock DESC, id) = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        SUM(quantity) OVER (PARTITION BY product_id, store_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
        SUM(quantity) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity_2,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) AS event_seq,
        FIRST_VALUE(quantity) IGNORE NULLS OVER (PARTITION BY store_id, product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity
    FROM ranked
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        running_quantity,
        running_quantity_2,
        first_known_quantity,
        event_seq - running_quantity_2 AS event_seq_minus_running_quantity_2,
        DATE_ADD(last_restock, 21) AS last_restock_plus_21d
    FROM scored
    WHERE last_restock > DATE '2023-09-13'
      AND event_seq >= 7
),
report AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        running_quantity_2,
        first_known_quantity,
        event_seq_minus_running_quantity_2,
        last_restock_plus_21d
    FROM valid
)
SELECT
    id,
    product_id,
    quantity,
    running_quantity_2,
    event_seq_minus_running_quantity_2,
    last_restock_plus_21d
FROM report
ORDER BY id DESC;
