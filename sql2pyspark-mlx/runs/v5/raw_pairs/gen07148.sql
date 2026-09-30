select p.id, p.category, p.product_name, p.brand from products p where p.supplier_id in (select id from suppliers where supplier_name is null) order by category desc, product_name
