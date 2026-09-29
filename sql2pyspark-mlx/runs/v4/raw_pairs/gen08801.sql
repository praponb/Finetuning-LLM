WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        is_active,
        age,
        ROUND(credit_limit, 2) AS credit_limit_rounded,
        COALESCE(city, 'DIRECT') AS resolved_city
    FROM customers
    WHERE signup_date > DATE '2024-02-25'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(credit_limit_rounded, 1) OVER (ORDER BY age ASC, id ASC) AS prev_credit_limit_rounded,
        MIN(age) OVER w_recent AS city_min_age
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY city)
),
latest_match AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.started_at
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.started_at
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Ranking and gap features
SELECT
    id,
    name,
    country,
    city,
    segment,
    signup_date,
    is_active,
    age,
    credit_limit_rounded,
    resolved_city,
    prev_credit_limit_rounded,
    city_min_age,
    started_at,
    ROW_NUMBER() OVER (ORDER BY age DESC, id ASC) AS row_seq,
    ROW_NUMBER() OVER (ORDER BY signup_date ASC, id ASC) AS seq_no,
    NTILE(5) OVER (PARTITION BY segment ORDER BY age ASC, id ASC) AS age_bucket
FROM latest_match;
