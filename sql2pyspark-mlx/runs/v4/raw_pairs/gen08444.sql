SELECT *
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY category ORDER BY cost) AS row_num
    FROM products
    WHERE SIZE(tags) > 1
) AS t
WHERE row_num <= 2
ORDER BY brand;
