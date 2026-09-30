SELECT p.brand, p.price, s.country, s.rating FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id ORDER BY brand DESC, s.country DESC
