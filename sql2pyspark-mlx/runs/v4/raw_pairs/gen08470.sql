SELECT *
FROM (
    SELECT
        country,
        city,
        rating
    FROM suppliers
) src
PIVOT (
    AVG(rating) AS value_sum
    FOR city IN ('Chiang Mai' AS chiang_mai, 'London' AS london)
);
