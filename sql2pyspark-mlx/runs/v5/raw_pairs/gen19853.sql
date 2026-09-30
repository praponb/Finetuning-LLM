SELECT products.id, products.stock_qty, products.product_name, suppliers.supplier_name, suppliers.country FROM products LEFT OUTER JOIN suppliers ON products.supplier_id = suppliers.id ORDER BY id
