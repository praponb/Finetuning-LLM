WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS('-', status, method) AS status_method_key
    FROM payments
    WHERE order_id IS NOT NULL
),
joined AS (
    -- Attach orders attributes
    SELECT
        a.*,
        b.channel,
        b.status AS order_status
    FROM cleaned a
    JOIN orders b
        ON a.order_id = b.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC, id) AS dedup_rank
    FROM joined
    QUALIFY dedup_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_status ORDER BY amount, id) AS event_seq,
        NTILE(3) OVER (PARTITION BY order_id ORDER BY amount, id) AS amount_bucket
    FROM deduped
)
SELECT
    id,
    order_id,
    method,
    amount,
    status_method_key,
    channel,
    dedup_rank,
    amount_bucket
FROM scored
ORDER BY amount DESC;
