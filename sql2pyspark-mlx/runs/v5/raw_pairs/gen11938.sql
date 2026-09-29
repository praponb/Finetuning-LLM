SELECT *
FROM (
    SELECT
        city,
        region,
        sqft
    FROM stores
) src
PIVOT (
    MAX(sqft) AS amount,
    COUNT(1) AS cnt
    FOR region IN ('East', 'West', 'North')
)
