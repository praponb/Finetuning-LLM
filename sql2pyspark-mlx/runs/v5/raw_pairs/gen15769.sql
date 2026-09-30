WITH with_latest AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.subscription_monthly_fee
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.monthly_fee AS subscription_monthly_fee
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, segment ORDER BY credit_limit DESC NULLS FIRST, id) AS latest_rank
    FROM with_latest
    QUALIFY latest_rank = 1
),
with_last AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.email,
        s.country,
        s.city,
        s.segment,
        s.signup_date,
        s.age,
        s.latest_rank,
        x.session_device,
        x.started_at
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.device AS session_device, c.started_at
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY segment, city ORDER BY latest_rank, id) AS row_seq,
        NTILE(3) OVER (PARTITION BY session_device ORDER BY age DESC, id) AS age_bucket,
        LAST_VALUE(session_device, TRUE) OVER w_recent AS last_known_session_device,
        SUM(age) OVER (PARTITION BY segment ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age
    FROM with_last
    WINDOW w_recent AS (PARTITION BY country ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY seq_no DESC NULLS FIRST, id) AS rn
    FROM enriched
    QUALIFY rn = 1
)
SELECT
    id,
    email,
    country,
    city,
    signup_date,
    age,
    latest_rank,
    started_at,
    seq_no,
    CASE WHEN city RLIKE '^(Berlin|Chiang Mai)$' THEN 'GROUP_B' WHEN city RLIKE '^(Bangkok|New York)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS city_zone
FROM deduped
ORDER BY signup_date DESC
