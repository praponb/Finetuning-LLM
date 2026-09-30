SELECT s.*, top1.brand, top1.price FROM suppliers s JOIN LATERAL (SELECT c.brand, c.price FROM products c WHERE c.supplier_id = s.id ORDER BY c.cost DESC, c.id DESC LIMIT 1) top1 ON TRUE;
