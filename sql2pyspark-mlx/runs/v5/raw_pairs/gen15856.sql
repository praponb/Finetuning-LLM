WITH cleaned AS (
    SELECT *, CAST(credit_limit AS INT) AS credit_limit_int
    FROM customers
    WHERE city IS NOT NULL
)
SELECT
    CASE WHEN credit_limit >= 16000 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_tier,
    city,
    SIZE(COLLECT_SET(city)) AS num_cities,
    COUNT_IF(country = 'SG') AS sg_events,
    COUNT_IF(segment = 'Corporate') AS corporate_events
FROM cleaned
GROUP BY 1, 2
