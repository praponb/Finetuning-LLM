select label, count(*) as product_count from products lateral view explode(tags) t as label where category = 'Grocery' group by label;
