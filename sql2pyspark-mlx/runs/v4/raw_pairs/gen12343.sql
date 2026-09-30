WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        age,
        CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label,
        COALESCE(segment, 'UNKNOWN') AS segment_filled
    FROM customers
    WHERE signup_date > TO_DATE('2023-06-05')
      AND country IN ('UK', 'DE', 'JP')
),
matched_keys AS (
    -- Set difference on keys
    SELECT city, country
    FROM cleaned
    EXCEPT
    SELECT city, country
    FROM customers
    WHERE age > 35
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN matched_keys k ON s.city = k.city AND s.country = k.country
),
cleaned_customers AS (
    -- Basic cleansing and defaults
    SELECT
        name,
        email,
        country,
        city,
        segment,
        age,
        is_active_label,
        segment_filled,
        CONCAT(segment, '/', country) AS segment_country,
        COALESCE(city, 'NONE') AS city_filled
    FROM verified
    WHERE city IS NOT NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        name,
        email,
        city,
        segment,
        age,
        is_active_label,
        segment_filled,
        city_filled,
        TRIM(country) AS clean_country,
        CEIL(age) AS age_whole
    FROM cleaned_customers
    WHERE segment IS NOT NULL
)
SELECT
    name,
    city,
    age,
    is_active_label,
    segment_filled,
    city_filled,
    clean_country,
    age_whole,
    CASE WHEN city RLIKE '^(Chiang Mai|Berlin)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS city_zone
FROM valid
ORDER BY age_whole NULLS FIRST;
