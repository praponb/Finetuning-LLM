WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        CASE WHEN quantity >= 268 THEN 'High' WHEN quantity >= 32 THEN 'Medium' ELSE 'Low' END quantity_level
    FROM inventory
    WHERE last_restock >= DATE '2023-03-22'
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.*,
        b.cost,
        b.product_name
    FROM cleaned src
    LEFT JOIN products b
        ON src.product_id = b.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        cost
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY cost DESC, id) AS row_num
        FROM with_dims
    ) x
    WHERE row_num = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(cost, 0) cost_rounded,
        FLOOR(quantity) quantity_whole
    FROM latest
    WHERE last_restock > TO_DATE('2023-07-22')
      AND store_id IN (1, 6)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY cost DESC NULLS LAST, id) AS latest_rank
    FROM filtered
    QUALIFY latest_rank = 1
)
SELECT
    s.id,
    s.store_id,
    s.cost,
    s.cost_rounded,
    s.latest_rank,
    (SELECT AVG(t2.shipping_fee) FROM orders t2 WHERE t2.store_id = s.store_id) AS avg_order_shipping_fee
FROM deduped s
ORDER BY cost_rounded NULLS FIRST, avg_order_shipping_fee
