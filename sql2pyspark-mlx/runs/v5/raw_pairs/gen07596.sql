WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        age + credit_limit age_plus_credit_limit,
        CASE WHEN is_active THEN 'Yes' ELSE 'No' END is_active_label
    FROM customers
    WHERE signup_date > DATE '2023-04-04'
      AND city IS DISTINCT FROM 'Bangkok'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        email,
        country,
        city,
        signup_date,
        age_plus_credit_limit,
        is_active_label,
        MIN(age) OVER (PARTITION BY country, city) country_min_age,
        SUM(credit_limit) OVER (PARTITION BY country ORDER BY signup_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_credit_limit
    FROM cleaned
    WHERE country = 'UK'
),
cleaned_customers AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        country,
        city,
        signup_date,
        country_min_age,
        cumulative_credit_limit,
        CASE WHEN city = 'New York' THEN 1 ELSE 0 END is_new_york,
        COALESCE(city, 'OTHER') city_filled
    FROM with_windows
    WHERE city IS DISTINCT FROM 'Chiang Mai'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        email,
        country,
        country_min_age,
        cumulative_credit_limit,
        CUME_DIST() OVER w_recent cumulative_credit_limit_cume_dist,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY is_new_york DESC NULLS LAST) is_new_york_dense_rank
    FROM cleaned_customers
    WINDOW w_recent AS (ORDER BY cumulative_credit_limit ASC NULLS LAST)
)
-- Grouped metrics
SELECT
    country,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    COUNT(*) AS cnt
FROM enriched
GROUP BY country;
