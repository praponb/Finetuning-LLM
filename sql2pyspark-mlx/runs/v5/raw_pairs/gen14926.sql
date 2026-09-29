WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date DESC, id) AS row_num
    FROM products
    WHERE brand IN ('Stark', 'Acme', 'Initech')
)
SELECT *
FROM numbered
WHERE row_num <= 3
ORDER BY id;
