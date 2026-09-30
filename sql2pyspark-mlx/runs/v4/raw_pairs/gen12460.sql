WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(email)) email_title
    FROM customers
    WHERE city IS NOT NULL
      AND city IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY age DESC, id ASC) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.order_date >= s.signup_date
        )
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        country,
        segment,
        is_active,
        age,
        email_title,
        rn,
        TRIM(country) clean_country,
        CEIL(age) age_whole,
        COALESCE(segment, 'UNKNOWN') segment_filled
    FROM not_flagged
    WHERE signup_date > DATE '2023-10-31'
      AND city IS NOT NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        segment,
        is_active,
        age,
        email_title,
        clean_country,
        age_whole,
        segment_filled,
        CONCAT(segment, ' ', country) segment_country,
        FLOOR(rn) rn_whole,
        COALESCE(segment, 'NONE') segment_clean
    FROM valid
    WHERE rn > 1
)
SELECT
    id,
    name,
    email,
    age,
    email_title,
    clean_country,
    age_whole,
    segment_filled,
    rn_whole,
    CASE WHEN name RLIKE '^(Olivia Garcia|Henry Ford)$' THEN 'GROUP_B' WHEN name RLIKE '^(Alice Wong|bob smith)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS name_zone
FROM cleaned
ORDER BY id DESC NULLS LAST;
