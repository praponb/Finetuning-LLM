SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date, id) rnk
    FROM customers
    WHERE credit_limit IS NOT NULL
) r
WHERE rnk <= 2
ORDER BY name DESC
