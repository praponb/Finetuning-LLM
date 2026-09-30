WITH latest_match AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.cost
    FROM orders s
    JOIN LATERAL (
            SELECT c.cost
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.delivered_date <= s.order_date
            ORDER BY c.delivered_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(shipping_fee) IGNORE NULLS OVER (PARTITION BY channel, store_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_shipping_fee,
        CORR(amount, discount) OVER (PARTITION BY customer_id) AS corr_amount_discount,
        AVG(amount) OVER w_running AS amount_avg_2_prior
    FROM latest_match
    WINDOW w_running AS (PARTITION BY channel ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, channel ORDER BY amount NULLS LAST, id) AS rank_in_group
    FROM scored
    QUALIFY rank_in_group = 1
),
latest_match_orders AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.store_id,
        s.channel,
        s.discount,
        s.corr_amount_discount,
        s.amount_avg_2_prior,
        s.rank_in_group,
        top1.shipment_status
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.status AS shipment_status
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.delivered_date <= s.order_date
            ORDER BY c.delivered_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY amount_avg_2_prior DESC, id) AS dedup_rank
    FROM latest_match_orders
    QUALIFY dedup_rank = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        shipment_status,
        discount,
        corr_amount_discount
    FROM deduped
) src
UNPIVOT (
    value FOR metric IN (discount, corr_amount_discount)
);
