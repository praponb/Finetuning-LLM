select store_id, round(avg(shipping_fee), 2) avg_shipping_fee from orders where amount >= 1540 group by store_id
