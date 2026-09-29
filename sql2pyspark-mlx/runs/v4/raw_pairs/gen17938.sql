SELECT *
FROM (
    SELECT
        city,
        segment,
        country,
        credit_limit
    FROM customers
) src
PIVOT (
    AVG(credit_limit)
    FOR country IN ('JP', 'DE')
);
