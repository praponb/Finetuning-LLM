select id, status, shipping_fee, avg(shipping_fee) over (partition by status) as status_avg_shipping_fee from orders;
