WITH latest_match AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.email,
        s.city,
        s.segment,
        s.signup_date,
        s.is_active,
        s.age,
        x.session_country,
        x.session_browser
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.country AS session_country, c.browser AS session_browser
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Multi-level aggregation
SELECT
    CASE WHEN age >= 60 THEN 'HIGH' ELSE 'LOW' END AS age_tier,
    COUNT(*) AS event_count,
    STDDEV_SAMP(age) AS age_stddev,
    ROUND(AVG(age), 2) AS avg_age
FROM latest_match
GROUP BY CASE WHEN age >= 60 THEN 'HIGH' ELSE 'LOW' END
HAVING age_stddev < 1000000
ORDER BY age_stddev ASC NULLS LAST
