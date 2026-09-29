WITH not_flagged AS (
    SELECT s.*
    FROM orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.delivered_date < s.order_date
        )
),
metrics AS (
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
        AVG(discount) OVER w_hist AS discount_avg_5,
        RANK() OVER (PARTITION BY channel ORDER BY amount DESC NULLS LAST) AS amount_rank,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM not_flagged
    WINDOW w_hist AS (ORDER BY amount ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY customer_id ORDER BY amount DESC, id ASC)
),
attributed AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM metrics src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.order_date >= pr.starts_on
       AND src.order_date <= DATE_ADD(pr.ends_on, 7)
),
metrics_orders AS (
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
        discount_avg_5,
        amount_rank,
        seq_no,
        promo_id,
        promo_code,
        discount_pct,
        FIRST_VALUE(discount_pct, TRUE) OVER (PARTITION BY promo_code ORDER BY shipping_fee ASC, id ASC, promo_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount_pct,
        SUM(discount_avg_5) OVER w_running AS running_discount_avg_5,
        FIRST_VALUE(amount, TRUE) OVER (ORDER BY amount ASC, id ASC, promo_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY promo_id ASC, id ASC, promo_id ASC) AS event_seq
    FROM attributed
    WINDOW w_running AS (ORDER BY order_date ASC, id ASC, promo_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount_rank ASC NULLS LAST, id ASC, promo_id ASC) AS rank_in_group
        FROM metrics_orders
    ) x
    WHERE rank_in_group = 1
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.order_date,
    s.status,
    s.channel,
    s.discount,
    s.shipping_fee,
    s.discount_avg_5,
    (SELECT MIN(t2.discount) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS min_order_discount
FROM deduped s;
