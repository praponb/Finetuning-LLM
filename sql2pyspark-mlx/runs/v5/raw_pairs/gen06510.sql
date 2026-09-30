SELECT *
FROM (
    SELECT
        city,
        country,
        rating
    FROM suppliers
) src
PIVOT (
    MAX(rating)
    FOR country IN ('SG' AS sg, 'US' AS us)
);
