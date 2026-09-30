WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END is_failed
    FROM payments
    WHERE method != 'card'
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.amount,
        e.paid_at,
        e.status,
        d.status AS order_status
    FROM base e
    JOIN orders d
        ON e.order_id = d.id
    WHERE d.amount >= 490
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(paid_at AS DATE) paid_at_day
    FROM enriched
    WHERE order_id IS NOT NULL
      AND order_status IN ('PENDING', 'RETURNED', 'SHIPPED')
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        amount,
        status,
        order_status,
        paid_at_day
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id, status ORDER BY paid_at NULLS LAST) AS rank_in_group
        FROM cleaned
    ) t
    WHERE rank_in_group = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        paid_at_day,
        LAG(amount) OVER (PARTITION BY status ORDER BY amount, id) prev_amount,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount, id) seq_no
    FROM ranked
)
SELECT
    id,
    paid_at_day,
    prev_amount,
    seq_no
FROM with_windows
ORDER BY seq_no DESC;
