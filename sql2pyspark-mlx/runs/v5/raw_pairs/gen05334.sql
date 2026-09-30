select label, count(*) as product_count from products lateral view explode(tags) t as label group by label order by product_count desc, label
