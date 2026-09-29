WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        age,
        TRIM(city) AS clean_city
    FROM customers
    WHERE segment IS NOT NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        email,
        country,
        city,
        segment,
        signup_date,
        age,
        clean_city
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY signup_date DESC NULLS FIRST, id) = 1
),
filtered_customers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(age * 100, 2) AS adjusted_age,
        COALESCE(city, 'UNKNOWN') AS city_filled
    FROM latest
    WHERE signup_date >= DATE '2023-03-10'
      AND city IS DISTINCT FROM 'Chiang Mai'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        country,
        signup_date,
        age,
        clean_city,
        city_filled,
        AVG(adjusted_age) OVER (ORDER BY age, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS adjusted_age_avg_2_prior,
        ROW_NUMBER() OVER (PARTITION BY country, segment ORDER BY age DESC, id) AS seq_no,
        FIRST_VALUE(age, TRUE) OVER (PARTITION BY segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_age
    FROM filtered_customers
    WHERE signup_date >= '2024-02-24'
)
-- Grouped metrics
SELECT
    CASE WHEN age >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_tier,
    seq_no,
    ROUND(SUM(seq_no) / NULLIF(COUNT(CASE WHEN country = 'SG' THEN 1 END), 0), 2) AS seq_no_per_sg,
    STDDEV_SAMP(age) AS age_stddev,
    COUNT(*) AS event_count,
    GROUPING_ID() AS grouping_id
FROM with_windows
GROUP BY GROUPING SETS ((CASE WHEN age >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END, seq_no), (CASE WHEN age >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END), ());
