select stock_qty, category, cost, length(brand) brand_length from products where month(launch_date) in (2, 3, 4)
