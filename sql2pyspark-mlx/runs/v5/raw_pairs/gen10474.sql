WITH Ranked AS (
    SELECT
        id,
        country,
        signup_date,
        name,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) AS rn
    FROM customers
    WHERE city IN ('Bangkok', 'Berlin')
)
SELECT *
FROM Ranked
WHERE rn <= 3;
