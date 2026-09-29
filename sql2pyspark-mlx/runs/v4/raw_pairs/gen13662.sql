select id, supplier_id from products where category = 'Books' or length(product_name) >= 5 order by supplier_id desc, id limit 1
