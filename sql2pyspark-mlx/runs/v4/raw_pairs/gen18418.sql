select id, shipping_fee, status from orders where shipping_fee >= (select max(shipping_fee) from orders where store_id = 18) order by id desc
