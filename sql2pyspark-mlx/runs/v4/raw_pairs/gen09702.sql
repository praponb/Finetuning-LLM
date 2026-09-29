SELECT category, supplier_id, price
FROM products
WHERE (stock_qty < 208 AND brand = 'Stark') OR cost BETWEEN 590 AND 650
ORDER BY price DESC, supplier_id DESC;
