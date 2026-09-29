WITH numbered AS (
    SELECT
        id,
        region,
        opened_date,
        sqft,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY opened_date DESC) AS rnk
    FROM stores
)
SELECT *
FROM numbered
WHERE rnk <= 3
ORDER BY rnk
