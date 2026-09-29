SELECT
    id,
    city,
    credit_limit,
    is_active
FROM (
    SELECT
        id,
        city,
        credit_limit,
        is_active,
        RANK() OVER (PARTITION BY city ORDER BY credit_limit DESC) pos
    FROM customers
) ranked
WHERE pos <= 3
ORDER BY id, city;
