SELECT
    id,
    store_id,
    sale_date,
    revenue
FROM (
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY sale_date DESC) AS row_num
    FROM store_sales
) AS t
WHERE row_num <= 3
ORDER BY store_id DESC, sale_date;
