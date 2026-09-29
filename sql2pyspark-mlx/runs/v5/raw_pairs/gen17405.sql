select id, price, label from products lateral view explode(tags) t as label;
