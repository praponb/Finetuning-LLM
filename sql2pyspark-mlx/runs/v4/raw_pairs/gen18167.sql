WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        country,
        city,
        rating,
        SUBSTRING(country, 1, 2) AS country_prefix,
        CASE WHEN city IN ('Bangkok', 'London') THEN 'Core' ELSE 'Other' END AS city_group
    FROM suppliers
    WHERE country NOT IN ('SG', 'US', 'JP')
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        country,
        rating
    FROM valid
) src
PIVOT (
    AVG(rating) AS value_sum
    FOR country IN ('DE', 'US', 'SG')
)
