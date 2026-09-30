select id, supplier_id, price, category from (select *, dense_rank() over (partition by supplier_id order by price) as pos from products) as ranked where pos <= 3
