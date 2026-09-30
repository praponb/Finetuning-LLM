WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        shipping_fee * amount AS amount_with_shipping
    FROM orders
    WHERE order_date > DATE '2023-07-23'
)
-- Join a dimension and aggregate
SELECT
    dim.city,
    e.channel,
    COUNT(*) AS rows_total,
    ROUND(AVG(e.discount), 2) AS avg_discount,
    MIN(e.shipping_fee) AS min_shipping_fee,
    GROUPING_ID() AS level_id
FROM filtered e
INNER JOIN customers dim ON e.customer_id = dim.id
GROUP BY GROUPING SETS ((dim.city, e.channel), (dim.city), ())
