WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN country IN ('US', 'SG') THEN 'Core' ELSE 'Other' END AS country_group,
        UPPER(city) AS city_upper,
        COALESCE(city, 'OTHER') AS city_clean
    FROM customers
    WHERE is_active = FALSE
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        country,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        country_group,
        city_upper,
        UPPER(country) AS country_upper
    FROM cleaned
    WHERE segment IS NOT NULL
      AND segment != 'SMB'
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.started_at >= s.signup_date
        )
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        name,
        segment,
        signup_date,
        city_upper
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, segment ORDER BY age DESC, id) AS dedup_rank
        FROM eligible
    ) ranked
    WHERE dedup_rank = 1
)
SELECT
    id,
    name,
    city_upper
FROM deduped
