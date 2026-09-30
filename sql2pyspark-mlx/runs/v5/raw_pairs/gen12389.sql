select id, product_name, category, tag_name from products lateral view explode(tags) t as tag_name where supplier_id in (26, 31);
