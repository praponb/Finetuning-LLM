WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        FLOOR(quantity) AS quantity_whole
    FROM inventory
    WHERE product_id IN (1, 2)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        quantity,
        quantity_whole,
        LAST_DAY(last_restock) AS last_restock_month_end
    FROM base
    WHERE last_restock < '2023-12-01'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        quantity,
        quantity_whole,
        last_restock_month_end,
        ROW_NUMBER() OVER w_running AS row_seq,
        AVG(quantity) OVER w_hist AS quantity_avg_2,
        ROW_NUMBER() OVER w_part AS seq_no,
        LAST_VALUE(quantity) IGNORE NULLS OVER (ORDER BY quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
        NTILE(3) OVER w_running AS quantity_whole_bucket
    FROM valid
    WINDOW w_running AS (PARTITION BY store_id ORDER BY quantity_whole, id), w_hist AS (ORDER BY last_restock_month_end, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY store_id ORDER BY quantity_whole DESC, id)
)
SELECT
    id,
    store_id,
    quantity,
    quantity_whole,
    row_seq,
    seq_no,
    quantity_whole_bucket
FROM metrics
ORDER BY quantity_whole_bucket NULLS FIRST
