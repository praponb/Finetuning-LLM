WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS key, attr_value
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        ARRAY_CONTAINS(coupon_codes, 'WELCOME5') AS is_welcome5,
        DATE_TRUNC('month', created_at) AS created_at_period
    FROM exploded
    WHERE created_at >= DATE '2025-03-02'
      AND HOUR(created_at) BETWEEN 6 AND 9
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        coupon_codes,
        key,
        is_welcome5,
        created_at_period,
        FIRST_VALUE(key, TRUE) OVER w_running AS first_known_key,
        FIRST_VALUE(key) IGNORE NULLS OVER (PARTITION BY key ORDER BY created_at_period, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_key_2,
        ROW_NUMBER() OVER (ORDER BY created_at DESC, id, key) AS event_seq
    FROM cleaned
    WINDOW w_running AS (ORDER BY created_at_period, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        coupon_codes,
        key,
        first_known_key,
        first_known_key_2,
        event_seq,
        LEAD(event_seq) OVER w_recent AS next_event_seq,
        LAG(event_seq) OVER w_recent AS prev_event_seq,
        ROW_NUMBER() OVER w_recent AS event_seq_2
    FROM enriched
    WINDOW w_recent AS (PARTITION BY key ORDER BY event_seq, id, key)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY key ORDER BY event_seq DESC NULLS LAST) AS event_seq_rank,
        NTILE(5) OVER (PARTITION BY key ORDER BY event_seq DESC, id, key) AS event_seq_bucket,
        NTILE(4) OVER (PARTITION BY key ORDER BY event_seq_2 DESC, id, key) AS event_seq_2_bucket,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY event_seq DESC, id, key) AS seq_no
    FROM with_windows
)
SELECT
    id,
    coupon_codes,
    key,
    first_known_key,
    first_known_key_2,
    prev_event_seq,
    event_seq_2,
    event_seq_2_bucket,
    CASE WHEN key RLIKE '^(device|gift)$' THEN 'CORE' ELSE 'GROUP_B' END AS key_zone
FROM metrics;
