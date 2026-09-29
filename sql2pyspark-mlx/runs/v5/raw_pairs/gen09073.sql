WITH base AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        CONCAT_WS('/', country, city) AS country_city_key
    FROM customers
    WHERE NOT is_active
)
SELECT
    CASE WHEN age >= 66 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_tier,
    COUNT_IF(segment = 'Consumer') AS consumer_events,
    COUNT(CASE WHEN country = 'DE' THEN 1 END) AS de_count,
    APPROX_PERCENTILE(age, 0.9) AS age_p90
FROM base
GROUP BY age_tier
