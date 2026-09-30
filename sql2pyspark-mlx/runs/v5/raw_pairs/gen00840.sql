select id, product_name, price, explode(tags) as tag from products where category in ('Sports', 'Books', 'Electronics')
