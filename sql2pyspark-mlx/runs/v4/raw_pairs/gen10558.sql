SELECT product_id, max_units, store_sale_count
FROM (
    SELECT product_id, MAX(units) AS max_units, COUNT(*) AS store_sale_count
    FROM store_sales
    GROUP BY product_id
) AS agg
WHERE max_units > 20
