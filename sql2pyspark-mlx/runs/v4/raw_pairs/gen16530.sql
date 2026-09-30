select products.id, products.cost from products where products.id in (select product_id from order_items);
