WITH enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.age,
        b.city,
        b.country
    FROM subscriptions s
    LEFT JOIN customers b
        ON s.customer_id = b.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(plan)) AS plan_title,
        COALESCE(city, 'N/A') AS resolved_city
    FROM enriched
    WHERE end_date > DATE '2023-04-26'
      AND city IS NOT NULL
      AND age > 46
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, city ORDER BY age DESC NULLS LAST, id) AS rn
    FROM valid
    QUALIFY rn = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN monthly_fee >= 240 THEN 'LARGE' ELSE 'SMALL' END AS monthly_fee_tier,
    CASE WHEN age >= 56 THEN 'HIGH' ELSE 'LOW' END AS age_tier,
    DATE_TRUNC('QUARTER', end_date) AS quarter_start,
    COUNT_IF(status = 'ACTIVE') AS active_events,
    APPROX_PERCENTILE(monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_quartiles,
    SUM(CASE WHEN city = 'London' THEN monthly_fee ELSE 0 END) AS london_monthly_fee,
    COUNT(*) AS event_count,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    SUM(CASE WHEN plan = 'standard' THEN age ELSE 0 END) AS standard_age,
    GROUPING(CASE WHEN monthly_fee >= 240 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_level
FROM latest
GROUP BY ROLLUP (CASE WHEN monthly_fee >= 240 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN age >= 56 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('QUARTER', end_date))
