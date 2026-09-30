SELECT
    id,
    country,
    age,
    email
FROM (
    SELECT
        id,
        country,
        age,
        email,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY age DESC, id) AS rn
    FROM customers
) AS r
WHERE rn <= 1
ORDER BY age;
