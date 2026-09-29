WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(method) AS method_upper,
        COALESCE(method, 'DIRECT') AS method_filled
    FROM payments
    WHERE method IS NOT NULL
),
enriched AS (
    -- Attach orders attributes
    SELECT e.*, d.channel
    FROM filtered e
    LEFT JOIN orders d
        ON e.order_id = d.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        method,
        amount,
        method_upper,
        method_filled,
        channel,
        LAG(amount) OVER (PARTITION BY order_id ORDER BY amount, id) AS prev_amount,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount, id) AS seq_no
    FROM enriched
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        method,
        amount,
        method_upper,
        method_filled,
        channel,
        prev_amount,
        seq_no,
        LOWER(method) AS method_lower,
        ROUND(amount, 0) AS amount_rounded
    FROM scored
    WHERE method IS NOT NULL
      AND amount BETWEEN 420 AND 970
)
SELECT
    id,
    method,
    amount,
    method_filled,
    channel,
    prev_amount,
    seq_no,
    amount_rounded
FROM cleaned
ORDER BY method;
