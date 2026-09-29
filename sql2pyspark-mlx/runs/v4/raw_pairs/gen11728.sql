WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS k, v
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY created_at, id, k) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at, id, k) AS event_seq_2
    FROM flattened
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(event_seq, 1) OVER (ORDER BY event_seq_2, id, k) AS next_event_seq,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY customer_id), 4) AS event_seq_share,
        COUNT(event_seq) OVER (PARTITION BY k) AS k_count_event_seq,
        LAST_VALUE(event_seq_2) IGNORE NULLS OVER (ORDER BY created_at, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_seq_2,
        RANK() OVER (ORDER BY event_seq NULLS FIRST) AS event_seq_rank
    FROM scored
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY customer_id, k ORDER BY event_seq) AS rn
    FROM metrics
    QUALIFY rn = 1
)
SELECT
    id,
    coupon_codes,
    k,
    event_seq,
    next_event_seq,
    event_seq_share,
    k_count_event_seq,
    event_seq_rank,
    rn
FROM latest
ORDER BY event_seq_rank DESC NULLS FIRST
