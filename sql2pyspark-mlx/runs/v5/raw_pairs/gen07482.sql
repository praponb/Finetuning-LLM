SELECT id, brand, stock_qty FROM products WHERE stock_qty <> 354 INTERSECT SELECT id, brand, stock_qty FROM products WHERE brand = 'Globex' ORDER BY brand DESC
