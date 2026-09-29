WITH base AS (
    SELECT
        *,
        CASE WHEN country IN ('SG', 'US') THEN 'Priority' ELSE 'Other' END AS country_group,
        COALESCE(segment, 'NONE') AS segment_clean
    FROM customers
    WHERE signup_date >= DATE '2023-02-26'
      AND LENGTH(name) >= 8
)
SELECT
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    CASE WHEN credit_limit >= 14600 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_tier,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    PERCENTILE_APPROX(age, ARRAY(0.25, 0.5, 0.75)) AS age_quartiles,
    COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) AS chiang_mai_count,
    COUNT(CASE WHEN country = 'SG' THEN 1 END) AS sg_count,
    CORR(credit_limit, age) AS corr_credit_limit_age,
    GROUPING_ID() AS grouping_level
FROM base
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', signup_date), CASE WHEN credit_limit >= 14600 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('QUARTER', signup_date)));
