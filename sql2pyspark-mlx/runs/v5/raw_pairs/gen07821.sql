WITH not_flagged AS (
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts x
            WHERE x.customer_id = s.id
              AND x.opened_date < s.signup_date
        )
)
SELECT
    CASE WHEN credit_limit >= 17900 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_tier,
    country,
    CASE WHEN age >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_tier,
    STDDEV_SAMP(credit_limit) AS credit_limit_stddev,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(age, 0.95) AS age_p95,
    COUNT_IF(country = 'JP') AS jp_events,
    AVG(age) FILTER (WHERE segment = 'Corporate') AS corporate_avg_age,
    CORR(age, credit_limit) AS corr_age_credit_limit
FROM not_flagged
GROUP BY CASE WHEN credit_limit >= 17900 THEN 'HIGH' ELSE 'LOW' END, country, CASE WHEN age >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END;
