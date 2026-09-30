WITH valid AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        CASE WHEN quantity >= 268 THEN 'High' WHEN quantity >= 42 THEN 'Medium' ELSE 'Low' END AS quantity_level
    FROM inventory
    WHERE quantity BETWEEN 89 AND 232
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity DESC, id) AS row_num
        FROM valid
    ) ranked
    WHERE row_num = 1
),
scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY row_num DESC, id) AS event_seq,
        SUM(quantity) OVER (ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity
    FROM latest
),
enriched AS (
    SELECT e.*, p.store_name
    FROM scored e
    LEFT JOIN stores p
        ON e.store_id = p.id
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY running_quantity, id) AS latest_rank
        FROM enriched
    ) t
    WHERE latest_rank = 1
),
enriched_inventory AS (
    SELECT
        s.*,
        b.cost,
        b.brand,
        b.price
    FROM ranked s
    INNER JOIN products b
        ON s.product_id = b.id
)
SELECT
    id,
    quantity,
    quantity_level,
    running_quantity,
    store_name,
    latest_rank,
    cost,
    brand,
    price
FROM enriched_inventory
ORDER BY quantity, latest_rank;
