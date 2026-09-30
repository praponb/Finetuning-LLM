SELECT *
FROM (
    SELECT
        segment,
        city,
        credit_limit
    FROM customers
) src
PIVOT (
    SUM(credit_limit) AS value_sum,
    COUNT(1) AS item_count
    FOR city IN ('Singapore' AS singapore, 'New York' AS new_york)
);
