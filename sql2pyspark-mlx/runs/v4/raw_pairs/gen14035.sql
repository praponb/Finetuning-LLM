select brand, percentile_approx(stock_qty, array(0.25, 0.5, 0.75)) as stock_qty_quartiles, stddev_samp(price) as price_stddev from products group by brand having stddev_samp(price) > 0
