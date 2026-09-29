select id, price, tag_name from products lateral view explode(tags) t as tag_name where array_contains(tags, 'eco')
