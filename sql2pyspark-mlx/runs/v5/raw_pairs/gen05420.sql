select id, product_name, explode(tags) as tag from products where cost < 620;
