WITH valid AS (
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        FLOOR(unit_price) unit_price_whole
    FROM order_items
    WHERE quantity < 7
),
joined AS (
    SELECT s.*, d.shipping_fee
    FROM valid s
    LEFT JOIN orders d
        ON s.order_id = d.id
),
scored AS (
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        unit_price_whole,
        shipping_fee,
        FIRST_VALUE(unit_price_whole, TRUE) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_unit_price_whole,
        STDDEV_SAMP(shipping_fee) OVER (PARTITION BY order_id) order_id_stddev_samp_shipping_fee,
        SUM(unit_price_whole) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_unit_price_whole,
        NTILE(4) OVER (PARTITION BY order_id ORDER BY shipping_fee DESC, id) shipping_fee_bucket
    FROM joined
),
enriched AS (
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        unit_price_whole,
        shipping_fee,
        first_known_unit_price_whole,
        order_id_stddev_samp_shipping_fee,
        cumulative_unit_price_whole,
        shipping_fee_bucket,
        AVG(unit_price_whole) OVER (PARTITION BY order_id ORDER BY cumulative_unit_price_whole, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) unit_price_whole_avg_6,
        AVG(unit_price) OVER (PARTITION BY order_id) order_id_avg_unit_price
    FROM scored
),
deduped AS (
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY first_known_unit_price_whole DESC NULLS LAST) AS rn
    FROM enriched
    QUALIFY rn = 1
)
SELECT
    id,
    order_id,
    quantity,
    unit_price,
    shipping_fee,
    first_known_unit_price_whole,
    order_id_stddev_samp_shipping_fee,
    unit_price_whole_avg_6,
    order_id_avg_unit_price
FROM deduped
ORDER BY unit_price_whole_avg_6 NULLS FIRST;
