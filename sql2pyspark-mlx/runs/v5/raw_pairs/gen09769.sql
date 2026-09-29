select id, supplier_id, price, dense_rank() over (partition by supplier_id order by price desc) as price_dense_rank from products;
