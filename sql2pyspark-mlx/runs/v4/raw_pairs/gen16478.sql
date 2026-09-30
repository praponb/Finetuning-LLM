select id, product_name, explode(tags) as label from products where year(launch_date) = 2023;
