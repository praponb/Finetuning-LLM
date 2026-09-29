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
        is_active,
        credit_limit,
        age,
        CONCAT_WS(',', email, country) AS email_country_key,
        LENGTH(country) AS country_length
    FROM customers
    WHERE signup_date >= DATE '2023-08-17'
      AND email LIKE '%@outlook.com'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        country,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        email_country_key,
        country_length,
        CONCAT(city, '/', name) AS city_name,
        COALESCE(city, 'NONE') AS city_clean
    FROM filtered
    WHERE signup_date > DATE '2023-03-10'
      AND city IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment, country ORDER BY country_length DESC, id ASC) AS dedup_rank
        FROM valid
    ) t
    WHERE dedup_rank = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, country
    FROM deduped
    EXCEPT DISTINCT
    SELECT id, country
    FROM customers
    WHERE city NOT IN ('Bangkok', 'Tokyo', 'Berlin')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN matched_keys k ON s.id = k.id AND s.country = k.country
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE EXISTS (
            SELECT 1
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.status != 'CLOSED'
        )
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions x
            WHERE x.customer_id = s.id
              AND x.start_date >= s.signup_date
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT *, MAX(credit_limit) OVER w_running AS segment_max_credit_limit
    FROM kept
    WHERE credit_limit <= 7300
    WINDOW w_running AS (PARTITION BY segment)
),
summary AS (
    -- Multi-level aggregation
    SELECT
        country,
        DATE_TRUNC('YEAR', signup_date) AS year_start,
        APPROX_PERCENTILE(segment_max_credit_limit, ARRAY(0.25, 0.5, 0.75)) AS segment_max_credit_limit_quartiles,
        SIZE(COLLECT_SET(segment)) AS num_segments
    FROM scored
    GROUP BY country, DATE_TRUNC('YEAR', signup_date)
    HAVING num_segments >= 1
)
SELECT
    country,
    year_start,
    num_segments,
    CASE WHEN country RLIKE '^(JP|UK)$' THEN 'GROUP_B' WHEN country RLIKE '^(TH|US)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS country_zone
FROM summary;
