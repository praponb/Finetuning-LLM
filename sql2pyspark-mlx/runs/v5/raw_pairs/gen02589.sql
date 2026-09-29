WITH latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY revenue NULLS FIRST, id) AS row_num
        FROM store_sales
    ) ranked
    WHERE row_num = 1
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.units,
    s.revenue,
    s.row_num,
    (SELECT MIN(t2.units) FROM store_sales t2 WHERE t2.product_id = s.product_id) AS min_store_sale_units
FROM latest s
ORDER BY units DESC
