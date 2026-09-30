WITH remaining_keys AS (
    -- Set difference on keys
    SELECT id, method
    FROM payments
    EXCEPT DISTINCT
    SELECT id, method
    FROM payments
    WHERE amount < 1500
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM payments s
    JOIN remaining_keys k ON s.id = k.id AND s.method = k.method
),
matched_keys AS (
    -- Set intersection on keys
    SELECT order_id, method
    FROM reconciled
    INTERSECT
    SELECT order_id, method
    FROM payments
    WHERE method IS NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    JOIN matched_keys k ON s.order_id = k.order_id AND s.method = k.method
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.*,
        b.channel,
        b.status AS order_status
    FROM kept_rows e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM joined a
    JOIN promotions pr
        ON a.channel = pr.channel
       AND a.paid_at BETWEEN pr.starts_on AND pr.ends_on
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.amount,
    s.paid_at,
    s.status,
    s.order_status,
    s.promo_id,
    s.discount_pct,
    (SELECT MIN(t2.amount) FROM payments t2 WHERE t2.order_id = s.order_id) AS min_payment_amount
FROM with_window s
ORDER BY promo_id NULLS LAST;
