select avg(quantity) as avg_quantity from order_items where product_id is not null;
