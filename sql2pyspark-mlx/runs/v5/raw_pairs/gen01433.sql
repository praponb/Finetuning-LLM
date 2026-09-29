select product_id, bit_or(units) as units_bit_or, stddev_samp(units) as units_stddev, bit_or(units) as units_bit_or_2, bit_and(units) as units_bit_and from store_sales group by product_id
