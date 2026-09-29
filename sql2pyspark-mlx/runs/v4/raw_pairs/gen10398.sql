select id, category, label from products lateral view explode(tags) t as label where stock_qty between 247 and 320;
