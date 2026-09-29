WITH valid AS (
    SELECT
        id,
        order_id,
        quantity,
        CEIL(unit_price) AS unit_price_whole
    FROM order_items
    WHERE product_id IN (7, 32, 36, 39)
),
with_windows AS (
    SELECT
        id,
        order_id,
        quantity,
        unit_price_whole,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price_whole DESC, id) AS event_seq,
        NTILE(5) OVER (PARTITION BY order_id ORDER BY unit_price_whole, id) AS unit_price_whole_bucket
    FROM valid
),
joined AS (
    SELECT a.*, b.discount
    FROM with_windows a
    INNER JOIN orders b
        ON a.order_id = b.id
),
enriched AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY quantity) AS quantity_dense_rank,
        LAG(discount, 1) OVER (PARTITION BY order_id ORDER BY event_seq, id) AS prev_discount
    FROM joined
),
base AS (
    SELECT
        id,
        order_id,
        quantity,
        unit_price_whole,
        event_seq,
        unit_price_whole_bucket,
        discount,
        quantity_dense_rank,
        ROUND(prev_discount * 0.85, 2) AS prev_discount_adjusted,
        CEIL(discount) AS discount_whole
    FROM enriched
    WHERE unit_price_whole <= 31
)
SELECT
    s.id,
    s.order_id,
    s.quantity,
    s.unit_price_whole,
    s.event_seq,
    s.unit_price_whole_bucket,
    s.quantity_dense_rank,
    s.discount_whole,
    (SELECT MIN(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS min_shipment_cost
FROM base s
ORDER BY quantity DESC;
