SELECT *
FROM (
    SELECT
        city,
        country,
        rating
    FROM suppliers
) src
PIVOT (
    AVG(rating) AS amount,
    COUNT(1) AS item_count
    FOR country IN ('SG' AS sg, 'JP' AS jp)
)
