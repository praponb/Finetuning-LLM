WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        country,
        segment,
        is_active,
        age,
        CAST(credit_limit AS INT) AS credit_limit_int,
        COALESCE(city, 'UNKNOWN') AS city_clean
    FROM customers
    WHERE email NOT LIKE '%@mail.co.th'
),
with_last AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.email,
        s.is_active,
        s.age,
        s.city_clean,
        x.session_browser
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.browser AS session_browser
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.customer_id = s.id
              AND x.status IN ('COMPLETED', 'RETURNED', 'SHIPPED')
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        email,
        is_active,
        city_clean,
        AVG(age) OVER w_running AS age_avg_6_prior,
        SUM(age) OVER (PARTITION BY session_browser ORDER BY age ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age,
        NTILE(4) OVER (PARTITION BY session_browser ORDER BY age ASC, id ASC) AS age_bucket
    FROM not_flagged
    WHERE session_browser = 'Chrome'
    WINDOW w_running AS (PARTITION BY session_browser ORDER BY age ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
not_flagged_customers AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.customer_id IS NOT NULL
        )
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM not_flagged_customers
    INTERSECT
    SELECT id
    FROM customers
    WHERE signup_date >= '2023-10-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM not_flagged_customers s
    JOIN remaining_keys k ON s.id = k.id
)
SELECT
    id,
    email,
    city_clean,
    age_avg_6_prior,
    running_age
FROM verified
ORDER BY running_age DESC
