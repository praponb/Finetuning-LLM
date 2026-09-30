SELECT discount, shipping_fee, amount, ADD_MONTHS(order_date, 3) AS order_date_plus_months FROM orders WHERE shipping_fee BETWEEN 13 AND 28
