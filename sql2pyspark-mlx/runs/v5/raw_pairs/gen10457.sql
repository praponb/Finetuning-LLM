select amount, status, least(amount, shipping_fee) as least_amount_shipping_fee from orders;
