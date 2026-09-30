select product_id, order_id, sum(quantity * unit_price) as revenue, round(stddev(quantity), 2) as stddev_quantity from order_items group by product_id, order_id order by stddev_quantity desc
