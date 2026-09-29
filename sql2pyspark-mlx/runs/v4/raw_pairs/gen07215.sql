select id, product_name, price, label from products lateral view explode(tags) t as label;
