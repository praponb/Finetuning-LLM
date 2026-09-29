WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        country,
        segment,
        signup_date,
        age,
        UPPER(segment) AS segment_upper,
        LOWER(segment) AS segment_lower
    FROM customers
    WHERE country IN ('SG', 'UK')
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) AS row_num
    FROM cleaned
    QUALIFY row_num = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN segment = 'Corporate' THEN 1 ELSE 0 END AS is_corporate,
        INITCAP(TRIM(segment)) AS segment_title,
        COALESCE(segment, 'UNKNOWN') AS resolved_segment
    FROM latest
    WHERE signup_date > DATE '2023-11-04'
      AND row_num <= 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER w_hist AS age_bucket,
        AVG(age) OVER (PARTITION BY segment ORDER BY row_num, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS age_avg_6_prior,
        AVG(is_corporate) OVER (ORDER BY age, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS is_corporate_avg_6,
        LAG(is_corporate, 2) OVER (PARTITION BY segment ORDER BY row_num, id) AS prev_is_corporate,
        ROW_NUMBER() OVER (PARTITION BY segment, country ORDER BY row_num DESC, id) AS seq_no
    FROM valid
    WINDOW w_hist AS (PARTITION BY segment, country ORDER BY age DESC, id)
),
cleaned_customers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        country,
        segment,
        age,
        segment_lower,
        is_corporate,
        segment_title,
        resolved_segment,
        age_avg_6_prior,
        is_corporate_avg_6,
        prev_is_corporate,
        GREATEST(is_corporate_avg_6, age_avg_6_prior) AS greatest_is_corporate_avg_6_age_avg_6_prior,
        LOWER(country) AS country_lower,
        COALESCE(segment, 'DIRECT') AS segment_filled
    FROM enriched
    WHERE signup_date > '2023-03-01'
),
valid_customers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        country,
        segment,
        age,
        is_corporate,
        segment_title,
        resolved_segment,
        is_corporate_avg_6,
        prev_is_corporate,
        greatest_is_corporate_avg_6_age_avg_6_prior,
        country_lower,
        segment_filled,
        GREATEST(prev_is_corporate, age_avg_6_prior) AS greatest_prev_is_corporate_age_avg_6_prior
    FROM cleaned_customers
    WHERE segment IS NOT NULL
),
latest_customers AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment, country ORDER BY greatest_is_corporate_avg_6_age_avg_6_prior NULLS FIRST, id) AS rn
        FROM valid_customers
    ) t
    WHERE rn = 1
)
SELECT
    id,
    country,
    segment,
    is_corporate,
    resolved_segment,
    is_corporate_avg_6,
    prev_is_corporate,
    greatest_is_corporate_avg_6_age_avg_6_prior,
    segment_filled,
    CASE WHEN segment RLIKE '^(Consumer|SMB)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS segment_zone
FROM latest_customers;
