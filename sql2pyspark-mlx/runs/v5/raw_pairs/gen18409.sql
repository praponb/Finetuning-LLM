select id, category, explode(tags) as tag from products where month(launch_date) in (7, 8, 10);
