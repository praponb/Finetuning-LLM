WITH enriched AS (
    -- Attach orders attributes
    SELECT src.*, d.order_date
    FROM payments src
    LEFT JOIN orders d
        ON src.order_id = d.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status
    FROM enriched
    EXCEPT
    SELECT status
    FROM payments
    WHERE method = 'promptpay'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.status = k.status
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        order_date,
        STDDEV_SAMP(amount) OVER (PARTITION BY method) AS method_stddev_samp_amount,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY amount NULLS LAST) AS amount_percent_rank,
        RANK() OVER (PARTITION BY order_id ORDER BY amount DESC) AS amount_rank
    FROM reconciled
)
SELECT
    s.order_id,
    s.paid_at,
    s.status,
    s.order_date,
    s.amount_percent_rank,
    s.amount_rank,
    (SELECT MAX(p2.amount) FROM payments p2 WHERE p2.order_id = s.order_id) AS max_payment_amount
FROM scored s
ORDER BY order_id;
