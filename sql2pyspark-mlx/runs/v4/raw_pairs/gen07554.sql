WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        DAY(last_restock) AS last_restock_day
    FROM inventory
    WHERE last_restock > DATE '2023-07-06'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY quantity DESC, id) AS row_num
        FROM filtered
    ) x
    WHERE row_num = 1
),
enriched AS (
    -- Attach products attributes
    SELECT a.*, b.product_name
    FROM ranked a
    LEFT JOIN products b
        ON a.product_id = b.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY quantity, id) AS event_seq,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY last_restock_day, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_avg_6_prior,
        NTILE(3) OVER (PARTITION BY store_id, product_id ORDER BY quantity DESC, id) AS quantity_bucket,
        NTILE(4) OVER (PARTITION BY product_id ORDER BY last_restock_day DESC, id) AS last_restock_day_bucket,
        AVG(last_restock_day) OVER (PARTITION BY product_id, store_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last_restock_day_avg_2
    FROM enriched
),
enriched_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        last_restock_day,
        row_num,
        product_name,
        event_seq,
        quantity_avg_6_prior,
        quantity_bucket,
        last_restock_day_bucket,
        last_restock_day_avg_2,
        LAST_VALUE(last_restock_day, TRUE) OVER (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_restock_day,
        AVG(event_seq) OVER (PARTITION BY product_id) AS product_id_avg_event_seq
    FROM with_windows
)
SELECT
    id,
    quantity,
    last_restock,
    last_restock_day,
    row_num,
    quantity_avg_6_prior,
    last_restock_day_bucket,
    last_restock_day_avg_2,
    last_known_last_restock_day
FROM enriched_inventory
ORDER BY last_restock_day DESC NULLS FIRST
