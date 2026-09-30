SELECT id, store_id
FROM store_sales
WHERE product_id IN (1, 7, 9) AND product_id = 1
ORDER BY id DESC, store_id DESC
