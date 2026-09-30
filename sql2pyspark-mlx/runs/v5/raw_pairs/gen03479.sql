select p.id, p.launch_date, p.product_name from products p where p.supplier_id in (select id from suppliers where supplier_name like '%City%')
