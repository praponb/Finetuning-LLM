SELECT
    id,
    email,
    signup_date,
    credit_limit,
    age
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date DESC, id) AS row_num
    FROM customers
) t
WHERE row_num = 1;
