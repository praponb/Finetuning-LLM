SELECT p.cost, p.price, s.city, s.rating, s.country FROM products p JOIN suppliers s ON p.supplier_id = s.id;
