WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(age * 1.15, 2) AS age_adjusted
    FROM customers
    WHERE signup_date >= TO_DATE('2023-07-12')
      AND city IS NOT NULL
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(country, ' - ', email) AS country_email,
        REPLACE(name, ' ', '_') AS name_slug
    FROM cleaned
    WHERE signup_date > DATE '2023-06-05'
      AND city IS NOT NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(segment, '/', name) AS segment_name,
        WEEKOFYEAR(signup_date) AS signup_date_weekofyear
    FROM base
    WHERE signup_date >= TO_DATE('2023-05-18')
      AND city IS NOT NULL
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, segment
    FROM filtered
    EXCEPT
    SELECT id, segment
    FROM customers
    WHERE email IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.id = k.id AND s.segment = k.segment
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.started_at >= s.signup_date
        )
)
SELECT
    id,
    name,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age,
    age_adjusted,
    country_email
FROM not_flagged;
