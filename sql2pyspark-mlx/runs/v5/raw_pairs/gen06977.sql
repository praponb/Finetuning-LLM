WITH cleaned AS (
    SELECT
        id,
        name,
        country,
        city,
        segment,
        is_active,
        credit_limit,
        age * credit_limit age_times_credit_limit,
        COALESCE(segment, 'NONE') segment_clean
    FROM customers
    WHERE signup_date >= DATE '2023-08-20'
),
ranked AS (
    SELECT
        id,
        country,
        city,
        credit_limit,
        segment_clean
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age_times_credit_limit NULLS LAST, id) = 1
),
with_last AS (
    SELECT
        s.id,
        s.country,
        s.credit_limit,
        s.segment_clean,
        top1.session_started_at,
        top1.session_country
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.started_at AS session_started_at, c.country AS session_country
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_country ORDER BY session_started_at NULLS FIRST, id) AS dedup_rank
    FROM with_last
    QUALIFY dedup_rank = 1
)
SELECT
    id,
    country,
    credit_limit,
    segment_clean,
    session_country,
    dedup_rank,
    CASE WHEN session_country RLIKE '^(JP|SG)$' THEN 'GROUP_B' WHEN session_country RLIKE '^(TH|US)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS session_country_zone
FROM latest;
