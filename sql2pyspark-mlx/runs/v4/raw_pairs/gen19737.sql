SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY units DESC NULLS LAST, id) AS rn
    FROM store_sales
) x
WHERE rn = 1;
