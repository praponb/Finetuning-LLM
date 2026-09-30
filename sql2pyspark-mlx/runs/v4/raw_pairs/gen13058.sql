SELECT *
FROM (
    SELECT
        country,
        city,
        rating
    FROM suppliers
) src
PIVOT (
    AVG(rating) AS total
    FOR city IN ('Singapore' AS singapore, 'Bangkok' AS bangkok, 'New York' AS new_york)
);
