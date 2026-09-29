WITH base AS (
    SELECT
        id,
        customer_id,
        order_date,
        status,
        amount,
        amount + shipping_fee AS amount_with_shipping
    FROM orders
    WHERE amount > 930
),
latest_match AS (
    SELECT s.*, top1.payment_amount
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.amount AS payment_amount
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    SELECT
        *,
        CUME_DIST() OVER w_grp AS amount_cume_dist,
        NTILE(3) OVER (PARTITION BY customer_id ORDER BY amount_with_shipping DESC, id) AS amount_with_shipping_bucket,
        ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY payment_amount, id) AS seq_no,
        SUM(amount_with_shipping) OVER w_grp_2 AS running_amount_with_shipping,
        SUM(amount_with_shipping) OVER w_recent AS cumulative_amount_with_shipping
    FROM latest_match
    WINDOW w_grp AS (PARTITION BY customer_id, status ORDER BY amount DESC NULLS FIRST), w_grp_2 AS (PARTITION BY status ORDER BY amount_with_shipping, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY status ORDER BY payment_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest_match_orders AS (
    SELECT s.*, top1.payment_status
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.status AS payment_status
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at <= s.order_date
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
joined AS (
    SELECT
        e.*,
        p.country,
        p.signup_date,
        p.age
    FROM latest_match_orders e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
latest_match_orders_2 AS (
    SELECT s.*, latest.shipment_cost
    FROM joined s
    JOIN LATERAL (
            SELECT c.cost AS shipment_cost
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.shipped_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    s.id,
    s.customer_id,
    s.order_date,
    s.status,
    s.amount_with_shipping,
    s.seq_no,
    s.running_amount_with_shipping,
    s.payment_status,
    s.signup_date,
    (SELECT AVG(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS avg_account_balance
FROM latest_match_orders_2 s;
