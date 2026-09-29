WITH latest AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity
    FROM order_items
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity NULLS LAST, id) = 1
),
remaining_keys AS (
    SELECT id, order_id
    FROM latest
    INTERSECT
    SELECT id, order_id
    FROM order_items
    WHERE product_id = 16
),
verified AS (
    SELECT s.*
    FROM latest s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.order_id = k.order_id
),
enriched AS (
    SELECT
        e.id,
        e.order_id,
        p.shipping_fee,
        p.discount
    FROM verified e
    LEFT JOIN orders p
        ON e.order_id = p.id
    WHERE p.amount IS NOT NULL
),
scored AS (
    SELECT
        id,
        discount,
        RANK() OVER (ORDER BY shipping_fee NULLS FIRST) AS shipping_fee_rank,
        CUME_DIST() OVER (ORDER BY discount DESC NULLS LAST) AS discount_cume_dist,
        STDDEV_SAMP(shipping_fee) OVER w_ordered AS order_id_stddev_samp_shipping_fee
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY order_id)
)
SELECT
    id,
    discount,
    shipping_fee_rank,
    order_id_stddev_samp_shipping_fee
FROM scored
ORDER BY shipping_fee_rank NULLS LAST
