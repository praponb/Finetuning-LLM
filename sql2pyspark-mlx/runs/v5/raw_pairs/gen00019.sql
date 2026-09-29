WITH valid AS (
    SELECT
        id,
        email,
        country,
        city,
        signup_date,
        is_active,
        credit_limit,
        age,
        TRIM(name) AS clean_name,
        COALESCE(segment, 'UNKNOWN') AS segment_clean
    FROM customers
    WHERE signup_date > DATE '2023-04-25'
)
SELECT country
FROM valid
INTERSECT
SELECT country
FROM customers
WHERE country IN ('UK', 'US');
