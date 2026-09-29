select tag_name, sum(stock_qty) as total_stock from products lateral view explode(tags) t as tag_name where product_name not like 'Sma%' group by tag_name order by total_stock desc, tag_name;
