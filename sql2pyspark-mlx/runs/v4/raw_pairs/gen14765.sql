select product_id, max(quantity) as max_quantity from order_items where quantity <= 8 and unit_price >= 110 and product_id in (1, 9, 21, 26) group by product_id order by product_id desc
