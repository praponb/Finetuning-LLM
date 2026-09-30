select tag, count(*) as product_count from products lateral view explode(tags) t as tag where launch_date >= '2024-09-01' group by tag order by product_count desc, tag;
