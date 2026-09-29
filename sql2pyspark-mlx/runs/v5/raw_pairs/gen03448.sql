WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(city) AS clean_city
    FROM customers
    WHERE signup_date > DATE '2023-02-23'
      AND city IS DISTINCT FROM 'Chiang Mai'
)
-- QUALIFY-style window filter
SELECT *
FROM valid
QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY age NULLS FIRST, id) = 1;
