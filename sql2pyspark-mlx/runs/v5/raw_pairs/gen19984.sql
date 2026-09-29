select shipping_fee, discount, status from orders where customer_id is null and (store_id in (15, 31, 38) or store_id is not null) order by shipping_fee desc
