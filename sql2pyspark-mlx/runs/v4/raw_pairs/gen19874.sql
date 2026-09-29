select label, sum(stock_qty) as total_stock from products lateral view explode(tags) t as label where month(launch_date) in (1, 2, 12) group by label order by total_stock desc, label;
