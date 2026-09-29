SELECT *
FROM (
    SELECT
        city,
        region,
        sqft
    FROM stores
) src
PIVOT (
    MAX(sqft)
    FOR region IN ('West' AS west, 'North' AS north, 'South' AS south)
);
