with lines as (select id, customer_id, created_at, key, value from carts lateral view outer explode(attributes) t as key, value) select created_at, value from lines;
