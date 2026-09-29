select category, min(launch_date) as min_launch_date from products where category in ('Sports', 'Fashion', 'Books') group by category order by min_launch_date desc, category;
