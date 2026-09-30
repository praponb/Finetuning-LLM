SELECT id, order_id, unit_price
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) row_num
    FROM order_items
) ranked
WHERE row_num <= 2
ORDER BY 3;
