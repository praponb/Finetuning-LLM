WITH kept AS (
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.started_at < s.signup_date
        )
)
SELECT
    DATE_TRUNC('YEAR', signup_date) AS year_start,
    PERCENTILE_APPROX(age, ARRAY(0.25, 0.5, 0.75)) AS age_quartiles,
    COUNT_IF(country = 'DE') AS de_events,
    CORR(age, credit_limit) AS corr_age_credit_limit
FROM kept
GROUP BY 1
HAVING COUNT_IF(country = 'DE') > 1
