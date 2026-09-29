WITH Ranked AS (
    SELECT
        id,
        category,
        launch_date,
        price,
        RANK() OVER (PARTITION BY category ORDER BY launch_date DESC) AS rn
    FROM products
)
SELECT *
FROM Ranked
WHERE rn = 1
ORDER BY rn DESC;
