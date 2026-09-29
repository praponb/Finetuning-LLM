select id, brand, price, label from products lateral view explode(tags) t as label where upper(product_name) like '%SB-%';
