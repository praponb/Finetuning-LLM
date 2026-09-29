SELECT id, supplier_id, launch_date
FROM (
    SELECT
        id,
        supplier_id,
        launch_date,
        ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY launch_date, id) AS row_num
    FROM products
) AS t
WHERE row_num <= 3
ORDER BY id DESC
