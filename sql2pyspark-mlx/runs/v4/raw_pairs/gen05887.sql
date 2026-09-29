select category, round(stddev(stock_qty), 2) as stddev_stock_qty from products group by category
