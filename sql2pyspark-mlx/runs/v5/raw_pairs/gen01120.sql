select id, price, brand, tag_name from products lateral view explode(tags) t as tag_name;
