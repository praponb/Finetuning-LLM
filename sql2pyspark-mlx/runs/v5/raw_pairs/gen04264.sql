select customer_id from orders where customer_id = 27 intersect select customer_id from orders where store_id = 9 order by customer_id
