WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        CAST(unit_price AS INT) AS unit_price_int
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.*,
        o.channel,
        o.order_date
    FROM cleaned a
    LEFT JOIN orders o
        ON a.order_id = o.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY unit_price DESC, id) AS latest_rank
    FROM with_dims
    QUALIFY latest_rank = 1
)
SELECT
    s.id,
    s.order_id,
    s.quantity,
    s.unit_price,
    s.unit_price_int,
    s.order_date,
    s.latest_rank,
    (SELECT MIN(o2.unit_price) FROM order_items o2 WHERE o2.order_id = s.order_id) AS min_order_item_unit_price
FROM deduped s
ORDER BY latest_rank NULLS FIRST
