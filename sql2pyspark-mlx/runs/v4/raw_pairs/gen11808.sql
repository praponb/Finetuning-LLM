SELECT
    id,
    segment,
    signup_date,
    country
FROM (
    SELECT
        id,
        segment,
        signup_date,
        country,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date DESC, id) AS rn
    FROM customers
) AS ranked
WHERE rn = 1
ORDER BY id DESC;
