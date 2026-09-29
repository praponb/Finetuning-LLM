WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        amount,
        status,
        SUM(amount) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status), 4) AS amount_share,
        DENSE_RANK() OVER (PARTITION BY method ORDER BY amount DESC NULLS LAST) AS amount_dense_rank
    FROM payments
),
enriched_payments AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.amount_share,
        b.discount,
        b.shipping_fee
    FROM enriched s
    LEFT JOIN orders b
        ON s.order_id = b.id
    WHERE b.status = 'RETURNED'
),
enriched_payments_2 AS (
    -- Attach orders attributes
    SELECT s.*, o.order_date
    FROM enriched_payments s
    LEFT JOIN orders o
        ON s.order_id = o.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched_payments_2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount_share DESC NULLS LAST, id) = 1
),
deduped_payments AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipping_fee, id) AS rank_in_group
    FROM deduped
    QUALIFY rank_in_group = 1
)
SELECT
    s.id,
    s.order_id,
    s.discount,
    s.rank_in_group,
    (SELECT SUM(p2.amount) FROM payments p2 WHERE p2.order_id = s.order_id) AS sum_payment_amount
FROM deduped_payments s;
