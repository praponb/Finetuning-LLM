SELECT *
FROM (
    SELECT
        city,
        country,
        rating
    FROM suppliers
) src
PIVOT (
    MAX(rating) AS total,
    COUNT(1) AS item_count
    FOR country IN ('UK', 'US')
)
