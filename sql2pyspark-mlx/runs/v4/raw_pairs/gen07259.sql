WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        is_active,
        DATE_ADD(signup_date, 30) AS signup_date_plus_30d,
        CONCAT(country, ' ', email) AS country_email
    FROM customers
    WHERE city IS NOT NULL
      AND segment IN ('Consumer', 'SMB', 'Corporate')
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date_plus_30d DESC, id) = 1
),
latest_customers AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY signup_date_plus_30d DESC) AS dedup_rank
        FROM latest
    ) ranked
    WHERE dedup_rank = 1
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_customers s
    WHERE EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.country IS NULL
        )
)
SELECT
    id,
    name,
    country,
    city,
    segment,
    signup_date,
    is_active
FROM eligible
ORDER BY segment DESC NULLS FIRST;
