SELECT amount, channel, order_date, GREATEST(amount, shipping_fee) AS greatest_amount_shipping_fee FROM orders WHERE shipping_fee BETWEEN 31 AND 37 AND discount >= 0.13;
