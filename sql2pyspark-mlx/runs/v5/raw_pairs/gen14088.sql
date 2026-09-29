SELECT i.id, i.product_id, i.quantity FROM inventory i WHERE i.quantity = (SELECT MAX(i2.quantity) FROM inventory i2 WHERE i2.product_id = i.product_id);
