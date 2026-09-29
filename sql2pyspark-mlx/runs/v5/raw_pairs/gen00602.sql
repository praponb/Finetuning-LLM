select id, brand, tags, size(tags) as tags_count from products where brand = 'Acme' and cost < 300 and year(launch_date) = 2024 order by id desc;
