WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at NULLS LAST, id) AS row_num
    FROM payments
    QUALIFY row_num = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS row_num_2
    FROM deduped
    QUALIFY row_num_2 = 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        row_num,
        row_num_2,
        CASE WHEN method = 'card' THEN 1 ELSE 0 END AS is_card,
        COALESCE(method, 'DIRECT') AS resolved_method
    FROM latest
    WHERE method IS DISTINCT FROM 'promptpay'
      AND paid_at > DATE '2023-08-14'
),
enriched AS (
    -- Attach orders attributes
    SELECT e.*, o.order_date
    FROM valid e
    INNER JOIN orders o
        ON e.order_id = o.id
)
-- Running and moving aggregates
SELECT
    id,
    order_id,
    amount,
    paid_at,
    status,
    is_card,
    LAG(amount) OVER w_recent AS prev_amount,
    LEAD(amount) OVER w_recent AS next_amount,
    LAG(is_card) OVER w_recent AS prev_is_card,
    LEAD(is_card) OVER w_recent AS next_is_card,
    ROW_NUMBER() OVER w_recent AS seq_no
FROM enriched
WINDOW w_recent AS (PARTITION BY status ORDER BY order_date, id);
