WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY quantity DESC, id) AS seq_no,
        NTILE(4) OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS quantity_bucket
    FROM inventory
)
SELECT
    id,
    store_id,
    quantity,
    seq_no
FROM scored;
