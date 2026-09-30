WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        CAST(quantity AS STRING) AS quantity_num,
        DATE_FORMAT(last_restock, 'MMM yyyy') AS last_restock_label
    FROM inventory
    WHERE store_id IN (2, 3)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_restock DESC NULLS LAST, id) AS row_num
        FROM valid
    ) x
    WHERE row_num = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(quantity) OVER (PARTITION BY product_id, store_id ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
        NTILE(4) OVER (PARTITION BY product_id, store_id ORDER BY quantity DESC, id) AS quantity_bucket
    FROM latest
)
SELECT
    id,
    store_id,
    quantity,
    last_restock,
    running_quantity,
    quantity_bucket
FROM with_windows
ORDER BY running_quantity DESC
