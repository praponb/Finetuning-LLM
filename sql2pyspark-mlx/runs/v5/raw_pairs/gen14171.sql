WITH metrics AS (
    SELECT
        id,
        unit_price,
        LAG(quantity) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS prev_quantity,
        LEAD(quantity) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS next_quantity,
        LEAD(unit_price) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS next_unit_price,
        LAG(unit_price) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS prev_unit_price,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price, id) AS event_seq,
        AVG(unit_price) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS unit_price_avg_3_prior
    FROM order_items
)
SELECT
    id,
    unit_price,
    prev_quantity,
    next_quantity,
    next_unit_price,
    prev_unit_price,
    event_seq,
    unit_price_avg_3_prior
FROM metrics;
