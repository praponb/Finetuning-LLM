select id, product_name, label from products lateral view explode(tags) t as label where upper(trim(product_name)) = 'RUNNING SHOES';
