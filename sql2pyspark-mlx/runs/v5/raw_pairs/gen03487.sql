WITH filtered AS (
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        is_active,
        age,
        ROUND(age, 2) AS age_rounded,
        COALESCE(city, 'DIRECT') AS city_filled
    FROM customers
    WHERE city NOT IN ('London', 'Singapore')
      AND city IS NOT NULL
)
SELECT
    id,
    country,
    signup_date,
    city_filled,
    LAG(age, 2) OVER (PARTITION BY country, city ORDER BY age_rounded, id) AS prev_age,
    ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY signup_date DESC, id) AS seq_no
FROM filtered
WHERE age_rounded >= 58
