SELECT *
FROM (
    SELECT
        id,
        segment,
        age,
        is_active,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age, id) AS row_num
    FROM customers
) AS r
WHERE row_num <= 5
ORDER BY id DESC
