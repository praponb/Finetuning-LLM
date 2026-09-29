SELECT id, quantity FROM inventory WHERE quantity < (SELECT MIN(quantity) FROM inventory WHERE quantity = 80)
