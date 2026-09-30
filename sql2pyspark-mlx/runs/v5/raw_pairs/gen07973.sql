WITH base AS (
    SELECT
        *,
        GREATEST(shipping_fee, amount) AS greatest_shipping_fee_amount,
        COALESCE(channel, 'N/A') AS channel_filled
    FROM orders
    WHERE order_date >= TO_DATE('2023-09-24')
      AND amount < 900
),
kept AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.order_id IN (8, 25, 26, 38)
        )
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY greatest_shipping_fee_amount DESC NULLS LAST, id) AS rank_in_group
        FROM kept
    ) t
    WHERE rank_in_group = 1
),
eligible AS (
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.delivered_date >= s.order_date
        )
),
latest_match AS (
    SELECT
        s.id,
        s.discount,
        s.shipping_fee,
        s.greatest_shipping_fee_amount,
        s.channel_filled,
        top1.shipment_status
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.status AS shipment_status
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.delivered_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    SELECT
        id,
        discount,
        greatest_shipping_fee_amount,
        channel_filled,
        RANK() OVER w_part AS greatest_shipping_fee_amount_rank,
        LAST_VALUE(shipping_fee) IGNORE NULLS OVER (PARTITION BY shipment_status ORDER BY greatest_shipping_fee_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_shipping_fee,
        DENSE_RANK() OVER w_hist AS discount_dense_rank
    FROM latest_match
    WHERE greatest_shipping_fee_amount <= 75
    WINDOW w_part AS (PARTITION BY shipment_status ORDER BY greatest_shipping_fee_amount DESC), w_hist AS (ORDER BY discount DESC)
),
base_orders AS (
    SELECT
        id,
        discount,
        channel_filled,
        greatest_shipping_fee_amount_rank,
        last_known_shipping_fee,
        discount_dense_rank,
        CASE WHEN greatest_shipping_fee_amount_rank >= 8 THEN 'High' WHEN greatest_shipping_fee_amount_rank >= 4 THEN 'Medium' ELSE 'Low' END AS greatest_shipping_fee_amount_rank_band,
        greatest_shipping_fee_amount + discount AS greatest_shipping_fee_amount_plus_discount
    FROM metrics
    WHERE discount_dense_rank > 8
)
SELECT
    CASE WHEN greatest_shipping_fee_amount_plus_discount >= 68 THEN 'HIGH' ELSE 'LOW' END AS greatest_shipping_fee_amount_plus_discount_tier,
    APPROX_PERCENTILE(discount, 0.9) AS discount_p90,
    PERCENTILE_APPROX(discount, 0.5) AS discount_p50
FROM base_orders
GROUP BY greatest_shipping_fee_amount_plus_discount_tier;
