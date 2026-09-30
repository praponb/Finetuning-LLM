with exploded as (select id, customer_id, created_at, inline(items) from carts) select id, customer_id, created_at, product_id, price from exploded order by price desc nulls last, id desc;
