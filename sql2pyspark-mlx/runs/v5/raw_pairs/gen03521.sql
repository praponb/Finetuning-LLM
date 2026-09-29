SELECT products.id, products.supplier_id, products.product_name, products.category, suppliers.country, suppliers.rating FROM products RIGHT JOIN suppliers ON products.supplier_id = suppliers.id;
