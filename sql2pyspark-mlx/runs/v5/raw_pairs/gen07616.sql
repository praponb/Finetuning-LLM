SELECT opened_date
FROM accounts
EXCEPT
SELECT sale_date
FROM store_sales
WHERE product_id IN (1, 2, 3, 6)
