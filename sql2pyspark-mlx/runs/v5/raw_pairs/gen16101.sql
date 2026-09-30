WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        AVG(unit_price) OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS unit_price_avg_6,
        ROW_NUMBER() OVER (ORDER BY unit_price ASC, id ASC) AS event_seq,
        CORR(quantity, unit_price) OVER (PARTITION BY product_id, order_id) AS corr_quantity_unit_price
    FROM order_items
    WHERE product_id IS NOT NULL
)
SELECT
    id,
    order_id,
    product_id,
    unit_price_avg_6,
    event_seq,
    corr_quantity_unit_price
FROM scored
ORDER BY unit_price_avg_6 DESC;
