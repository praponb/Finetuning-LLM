SELECT id, city, opened_date
FROM (
    SELECT
        id,
        city,
        opened_date,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY opened_date) AS row_num
    FROM stores
    WHERE region IN ('West', 'Central')
) AS r
WHERE row_num = 1;
