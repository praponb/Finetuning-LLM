SELECT *
FROM (
    SELECT
        region,
        city,
        sqft
    FROM stores
) src
PIVOT (
    SUM(sqft) AS total,
    COUNT(1) AS cnt
    FOR city IN ('Singapore', 'Berlin')
);
