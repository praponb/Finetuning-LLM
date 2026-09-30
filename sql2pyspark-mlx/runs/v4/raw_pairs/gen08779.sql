WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN segment = 'SMB' THEN 1 ELSE 0 END is_smb,
        COALESCE(segment, 'OTHER') segment_clean
    FROM customers
    WHERE credit_limit IS NOT NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        is_smb,
        segment_clean,
        SUBSTRING(country, 1, 2) country_prefix,
        REPLACE(name, ' ', '-') name_slug
    FROM valid
    WHERE signup_date > TO_DATE('2023-07-30')
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age DESC, id) AS rn
    FROM cleaned
    QUALIFY rn = 1
),
latest_customers AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY credit_limit DESC, id) AS rn_2
        FROM latest
    ) x
    WHERE rn_2 = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM latest_customers
    QUALIFY RANK() OVER (PARTITION BY city ORDER BY is_smb DESC) = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY rn, id) = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN is_smb >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_smb_tier,
    MAX(age) FILTER (WHERE segment = 'Consumer') AS consumer_max_age,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM deduped
GROUP BY CASE WHEN is_smb >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING consumer_max_age < 0
ORDER BY is_smb_tier DESC NULLS FIRST;
