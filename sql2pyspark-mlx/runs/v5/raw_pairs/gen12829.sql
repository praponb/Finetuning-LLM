WITH filtered AS (
    SELECT
        id,
        name,
        email,
        city,
        segment,
        signup_date,
        credit_limit,
        INITCAP(TRIM(segment)) AS segment_title,
        COALESCE(city, 'DIRECT') AS resolved_city
    FROM customers
    WHERE YEAR(signup_date) = 2023
      AND segment IS DISTINCT FROM 'Consumer'
)
SELECT
    id,
    credit_limit,
    segment_title,
    resolved_city
FROM filtered
QUALIFY ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date DESC, id) = 1
