WITH cleaned AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        quantity * unit_price AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
remaining_keys AS (
    SELECT id, order_id
    FROM cleaned
    INTERSECT
    SELECT id, order_id
    FROM order_items
    WHERE product_id IN (2, 14, 21, 29)
),
verified AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.order_id = k.order_id
),
enriched AS (
    SELECT e.*, p.shipping_fee
    FROM verified e
    INNER JOIN orders p
        ON e.order_id = p.id
),
valid AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        line_total,
        shipping_fee,
        CASE WHEN unit_price >= 370 THEN 'High' WHEN unit_price >= 60 THEN 'Medium' ELSE 'Low' END AS unit_price_level
    FROM enriched
    WHERE shipping_fee <= 12
),
summary AS (
    SELECT
        product_id,
        order_id,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT order_id) AS distinct_order_ids,
        APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles
    FROM valid
    GROUP BY product_id, order_id
    HAVING COUNT(DISTINCT id) > 1
)
SELECT
    s.product_id,
    s.order_id,
    s.distinct_ids,
    s.distinct_order_ids,
    (SELECT MIN(s2.cost) FROM shipments s2 WHERE s2.order_id = s.order_id) AS min_shipment_cost
FROM summary s
ORDER BY product_id DESC NULLS FIRST;
