WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        LEAST(quantity, unit_price) AS least_quantity_unit_price,
        unit_price * quantity AS line_total
    FROM order_items
    WHERE order_id = 28
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY line_total DESC NULLS LAST, id) = 1
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.*,
        o.status,
        o.order_date
    FROM latest src
    INNER JOIN orders o
        ON src.order_id = o.id
)
SELECT
    s.id,
    s.order_id,
    s.quantity,
    s.unit_price,
    s.least_quantity_unit_price,
    s.status,
    s.order_date,
    (SELECT SUM(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS sum_shipment_cost
FROM joined s
ORDER BY status;
