WITH base AS (
    SELECT *, CAST(discount AS INT) discount_int
    FROM orders
    WHERE status IN ('COMPLETED', 'SHIPPED', 'PENDING')
),
with_latest AS (
    SELECT
        s.*,
        latest.delivered_date,
        latest.shipment_shipped_date
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.delivered_date, c.shipped_date AS shipment_shipped_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.quantity < 4
        )
),
attributed AS (
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM not_flagged src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.order_date >= sub.start_date
       AND src.order_date < sub.end_date + INTERVAL '30' DAY
),
with_windows AS (
    SELECT
        id,
        customer_id,
        order_date,
        status,
        channel,
        amount,
        shipment_shipped_date,
        subscription_id,
        monthly_fee,
        LEAD(discount) OVER w_ordered next_discount,
        LAG(discount) OVER w_ordered prev_discount,
        LAG(monthly_fee) OVER w_ordered prev_monthly_fee,
        LEAD(monthly_fee) OVER w_ordered next_monthly_fee,
        ROW_NUMBER() OVER w_ordered event_seq
    FROM attributed
    WINDOW w_ordered AS (PARTITION BY store_id, plan ORDER BY shipment_shipped_date, id, subscription_id)
),
joined AS (
    SELECT
        a.id,
        a.customer_id,
        a.status,
        a.amount,
        a.subscription_id,
        a.prev_discount,
        a.next_monthly_fee,
        b.segment,
        b.name,
        b.country
    FROM with_windows a
    INNER JOIN customers b
        ON a.customer_id = b.id
),
deduped AS (
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id, subscription_id) = 1
)
SELECT
    *,
    NTILE(10) OVER (PARTITION BY country ORDER BY amount DESC, id, subscription_id) amount_bucket,
    NTILE(10) OVER (PARTITION BY country ORDER BY subscription_id DESC, id, subscription_id) subscription_id_bucket,
    SUM(next_monthly_fee) OVER (PARTITION BY status ORDER BY next_monthly_fee, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_next_monthly_fee
FROM deduped
