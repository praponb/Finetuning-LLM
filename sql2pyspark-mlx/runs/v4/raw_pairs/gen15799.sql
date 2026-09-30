select id, category, label from products lateral view explode(tags) t as label
