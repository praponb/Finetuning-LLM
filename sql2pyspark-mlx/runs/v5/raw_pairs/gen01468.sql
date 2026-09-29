SELECT cost, CONCAT(category, ' ', product_name) AS category_product_name
FROM products
WHERE stock_qty > 110 AND stock_qty < 362
