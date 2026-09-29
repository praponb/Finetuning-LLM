WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(discount) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS discount_avg_5_prior,
        LAST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY status ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
        CORR(shipping_fee, amount) OVER (PARTITION BY store_id) AS corr_shipping_fee_amount
    FROM orders
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY order_date DESC) = 1
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.quantity,
        top1.unit_price
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.quantity, c.unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Window metrics over several different partitions
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
    discount_avg_5_prior,
    last_known_channel,
    corr_shipping_fee_amount,
    quantity,
    unit_price,
    LAST_VALUE(quantity, TRUE) OVER (PARTITION BY status ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
    ROUND(quantity / SUM(quantity) OVER (PARTITION BY customer_id), 4) AS quantity_share,
    AVG(quantity) OVER (PARTITION BY store_id ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS quantity_avg_3_prior,
    LAST_VALUE(discount) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY corr_shipping_fee_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_discount,
    SUM(shipping_fee) OVER (PARTITION BY store_id, customer_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_shipping_fee
FROM with_last
