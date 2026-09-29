WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN credit_limit >= 14700 THEN 'High' WHEN credit_limit >= 2700 THEN 'Medium' ELSE 'Low' END AS credit_limit_band
    FROM customers
    WHERE segment IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        country,
        credit_limit
    FROM cleaned
) src
PIVOT (
    AVG(credit_limit) AS amount,
    COUNT(1) AS cnt
    FOR country IN ('UK', 'SG')
);
