WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(name, ' - ', country) AS name_country,
        COALESCE(segment, 'unassigned') AS segment_filled
    FROM customers
    WHERE age <= 31
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(credit_limit * 0.85, 2) AS adjusted_credit_limit,
        COALESCE(segment, 'NONE') AS resolved_segment
    FROM filtered
    WHERE city = 'New York'
),
scored AS (
    -- Window metrics over several different partitions
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
        name_country,
        segment_filled,
        adjusted_credit_limit,
        resolved_segment,
        NTILE(5) OVER (PARTITION BY city ORDER BY age, id) AS age_bucket,
        ROW_NUMBER() OVER (PARTITION BY segment, city ORDER BY age, id) AS event_seq,
        AVG(age) OVER (PARTITION BY city ORDER BY age, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS age_avg_5,
        LAG(credit_limit, 1) OVER (PARTITION BY city ORDER BY signup_date, id) AS prev_credit_limit,
        AVG(age) OVER (PARTITION BY city, segment ORDER BY credit_limit, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS age_avg_5_prior
    FROM cleaned
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN age >= 58 THEN 'High' WHEN age >= 32 THEN 'Medium' ELSE 'Low' END AS age_level,
        IF(is_active, 'Yes', 'No') AS is_active_label,
        COALESCE(city, 'DIRECT') AS city_clean
    FROM scored
    WHERE signup_date >= DATE '2023-11-02'
)
SELECT
    id,
    name,
    email,
    country,
    city,
    segment,
    age,
    name_country,
    segment_filled,
    CASE WHEN name RLIKE '^(Emma Brown|Noah Park)$' THEN 'GROUP_B' WHEN name RLIKE '^(Alice Wong|bob smith)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS name_zone
FROM base
ORDER BY age, segment DESC;
