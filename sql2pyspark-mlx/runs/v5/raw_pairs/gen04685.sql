WITH RankedCustomers AS (
    SELECT
        id,
        segment,
        signup_date,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date, id) AS rnk
    FROM customers
)
SELECT *
FROM RankedCustomers
WHERE rnk <= 5
ORDER BY rnk DESC;
