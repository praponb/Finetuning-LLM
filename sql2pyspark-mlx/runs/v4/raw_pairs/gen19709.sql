SELECT *
FROM (
    SELECT
        city,
        country,
        rating
    FROM suppliers
) src
PIVOT (
    SUM(rating) AS amount,
    COUNT(1) AS cnt
    FOR country IN ('SG' AS sg, 'DE' AS de)
)
