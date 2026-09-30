select id, supplier_id, count(*) over (partition by supplier_id) as supplier_id_count from products where category = 'Electronics';
