WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date, id) AS rank_in_group
        FROM customers
    ) ranked
    WHERE rank_in_group = 1
)
-- Grouped metrics
SELECT
    CASE WHEN age >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_tier,
    CASE WHEN credit_limit >= 17600 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    ROUND(SUM(age) / NULLIF(COUNT(CASE WHEN city = 'Berlin' THEN 1 END), 0), 2) AS age_per_berlin,
    PERCENTILE_APPROX(credit_limit, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_quartiles,
    COUNT(*) AS num_rows,
    ROUND(SUM(credit_limit) / NULLIF(COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END), 0), 2) AS credit_limit_per_chiang_mai,
    PERCENTILE_APPROX(age, ARRAY(0.25, 0.5, 0.75)) AS age_quartiles,
    GROUPING_ID() AS grouping_level
FROM deduped
GROUP BY GROUPING SETS ((CASE WHEN age >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN credit_limit >= 17600 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN age >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END), ());
