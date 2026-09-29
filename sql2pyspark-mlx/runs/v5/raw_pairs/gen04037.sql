SELECT *
FROM (
    SELECT
        region,
        city,
        sqft
    FROM stores
) src
PIVOT (
    MAX(sqft)
    FOR city IN ('Chiang Mai' AS chiang_mai, 'London' AS london)
);
