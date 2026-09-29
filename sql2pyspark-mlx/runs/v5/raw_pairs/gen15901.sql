with lines as (select id, customer_id, created_at, inline(items) from carts) select id, created_at, product_id, qty, price from lines order by product_id desc, qty
