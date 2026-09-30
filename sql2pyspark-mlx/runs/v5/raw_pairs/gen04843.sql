WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        product_id,
        LEAD(quantity) OVER w_part AS next_quantity,
        LEAD(unit_price) OVER w_part AS next_unit_price,
        LAG(unit_price) OVER w_part AS prev_unit_price,
        ROW_NUMBER() OVER w_part AS event_seq,
        LAG(quantity, 2) OVER w_hist AS prev_quantity
    FROM order_items
    WINDOW w_part AS (PARTITION BY product_id, order_id ORDER BY quantity, id), w_hist AS (ORDER BY quantity, id)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY next_quantity NULLS LAST, id) AS rank_in_group
    FROM enriched
    QUALIFY rank_in_group = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY next_unit_price DESC NULLS FIRST) AS rn
        FROM deduped
    ) ranked
    WHERE rn = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT order_id, id
    FROM latest
    EXCEPT
    SELECT order_id, id
    FROM order_items
    WHERE order_id IN (3, 14, 30, 37)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN matched_keys k ON s.order_id = k.order_id AND s.id = k.id
)
SELECT
    s.product_id,
    s.next_unit_price,
    s.prev_quantity,
    s.rn,
    (SELECT AVG(t2.quantity) FROM order_items t2 WHERE t2.product_id = s.product_id) AS avg_order_item_quantity
FROM reconciled s
ORDER BY product_id DESC;
