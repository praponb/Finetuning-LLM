WITH cleaned AS (
    SELECT
        id,
        name,
        country,
        city,
        segment,
        credit_limit,
        age,
        SUBSTRING(city, 1, 2) AS city_prefix,
        LENGTH(country) AS country_length,
        COALESCE(segment, 'UNKNOWN') AS segment_filled
    FROM customers
    WHERE NOT is_active
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment, city ORDER BY country_length, id) AS rank_in_group
        FROM cleaned
    ) t
    WHERE rank_in_group = 1
),
filtered AS (
    SELECT
        *,
        COALESCE(age, 0) AS age_filled,
        INITCAP(TRIM(country)) AS country_title,
        COALESCE(city, 'DIRECT') AS resolved_city
    FROM deduped
    WHERE name LIKE 'M%'
),
with_latest AS (
    SELECT s.*, latest.started_at
    FROM filtered s
    JOIN LATERAL (
            SELECT c.started_at
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    SELECT
        id,
        name,
        city,
        credit_limit,
        age,
        city_prefix,
        country_length,
        resolved_city,
        started_at
    FROM with_latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at, id) = 1
),
matched_keys AS (
    SELECT id, city
    FROM latest
    EXCEPT DISTINCT
    SELECT id, city
    FROM customers
    WHERE is_active = TRUE
),
verified AS (
    SELECT s.*
    FROM latest s
    INNER JOIN matched_keys k ON s.id = k.id AND s.city = k.city
),
eligible AS (
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.plan = 'premium'
        )
)
SELECT
    CASE WHEN country_length >= 79 THEN 'HIGH' ELSE 'LOW' END AS country_length_tier,
    DATE_TRUNC('YEAR', started_at) AS year_start,
    AVG(age) FILTER (WHERE city = 'Chiang Mai') AS chiang_mai_avg_age,
    SUM(country_length) AS total_country_length,
    MAX(started_at) AS last_started_at,
    MAX(credit_limit) FILTER (WHERE city = 'Chiang Mai') AS chiang_mai_max_credit_limit,
    MIN(country_length) AS min_country_length,
    PERCENTILE_APPROX(country_length, ARRAY(0.25, 0.5, 0.75)) AS country_length_quartiles,
    GROUPING_ID() AS grouping_level
FROM eligible
GROUP BY GROUPING SETS ((CASE WHEN country_length >= 79 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('YEAR', started_at)), (CASE WHEN country_length >= 79 THEN 'HIGH' ELSE 'LOW' END), ())
ORDER BY chiang_mai_avg_age;
