select id, product_name, explode(tags) as label from products where launch_date >= '2024-10-01'
