SELECT MIN(order_date) first_order_date FROM orders WHERE channel IN ('partner', 'mobile', 'store') AND discount <= 0.29
