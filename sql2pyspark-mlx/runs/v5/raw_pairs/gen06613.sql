SELECT
    id,
    segment,
    credit_limit,
    email
FROM (
    SELECT
        id,
        segment,
        credit_limit,
        email,
        DENSE_RANK() OVER (PARTITION BY segment ORDER BY credit_limit DESC) AS row_num
    FROM customers
) AS r
WHERE row_num = 1
ORDER BY credit_limit DESC, email
