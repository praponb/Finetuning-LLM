SELECT *
FROM (
    SELECT
        country,
        city,
        rating
    FROM suppliers
) src
PIVOT (
    AVG(rating) AS amount
    FOR city IN ('Singapore' AS singapore, 'Tokyo' AS tokyo, 'Chiang Mai' AS chiang_mai)
);
