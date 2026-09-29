WITH remaining_keys AS (
    SELECT browser, customer_id
    FROM sessions
    EXCEPT ALL
    SELECT browser, customer_id
    FROM sessions
    WHERE customer_id IN (7, 18)
),
verified AS (
    SELECT s.*
    FROM sessions s
    INNER JOIN remaining_keys k ON s.browser = k.browser AND s.customer_id = k.customer_id
),
enriched AS (
    SELECT
        s.id,
        s.customer_id,
        s.device,
        s.browser,
        d.credit_limit,
        d.signup_date
    FROM verified s
    LEFT JOIN customers d
        ON s.customer_id <=> d.id
),
kept AS (
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time < s.signup_date
        )
),
not_flagged AS (
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time <= s.signup_date
        )
)
SELECT
    CASE WHEN credit_limit >= 14100 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
    PERCENTILE_APPROX(credit_limit, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_quartiles,
    SIZE(COLLECT_SET(device)) AS num_devices,
    SUM(credit_limit) AS total_credit_limit
FROM not_flagged
GROUP BY credit_limit_tier
HAVING SUM(credit_limit) > 0;
