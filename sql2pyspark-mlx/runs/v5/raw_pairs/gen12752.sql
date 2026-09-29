WITH cleaned AS (
    SELECT
        id,
        order_date,
        status,
        amount,
        discount,
        shipping_fee,
        GREATEST(shipping_fee, amount) AS greatest_shipping_fee_amount
    FROM orders
    WHERE order_date > DATE '2023-02-26'
      AND store_id = 27
),
enriched AS (
    SELECT
        *,
        SUM(discount) OVER (PARTITION BY status) AS status_sum_discount,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY shipping_fee DESC NULLS LAST) AS shipping_fee_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS event_seq
    FROM cleaned
    WHERE amount BETWEEN 590 AND 1490
),
with_last AS (
    SELECT s.*, top1.shipment_delivered_date
    FROM enriched s
    JOIN LATERAL (
            SELECT c.delivered_date AS shipment_delivered_date
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.shipped_date <= s.order_date
            ORDER BY c.shipped_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
cleaned_orders AS (
    SELECT
        id,
        order_date,
        status,
        amount,
        discount,
        shipping_fee,
        greatest_shipping_fee_amount,
        status_sum_discount,
        event_seq,
        shipping_fee_percent_rank - discount AS shipping_fee_percent_rank_minus_discount
    FROM with_last
    WHERE status = 'COMPLETED'
),
enriched_orders AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY discount DESC, id) AS event_seq_2,
        AVG(greatest_shipping_fee_amount) OVER (PARTITION BY status ORDER BY status_sum_discount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS greatest_shipping_fee_amount_avg_3,
        LAST_VALUE(amount, TRUE) OVER w_ordered AS last_known_amount,
        SUM(amount) OVER (ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM cleaned_orders
    WINDOW w_ordered AS (ORDER BY shipping_fee_percent_rank_minus_discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    order_date,
    status,
    amount,
    shipping_fee,
    greatest_shipping_fee_amount,
    status_sum_discount,
    event_seq,
    shipping_fee_percent_rank_minus_discount
FROM enriched_orders
