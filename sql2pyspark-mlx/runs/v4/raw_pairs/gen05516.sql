select store_id, round(stddev(quantity), 2) stddev_quantity, count(distinct quantity) unique_products from inventory group by store_id having count(distinct quantity) >= 2
