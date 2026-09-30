select stock_qty, launch_date, sort_array(tags) as sorted_tags from products where category in ('Electronics', 'Home');
