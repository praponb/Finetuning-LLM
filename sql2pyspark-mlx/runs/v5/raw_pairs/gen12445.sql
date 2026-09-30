select store_id from orders where shipping_fee > 27 union select store_id from orders where channel = 'web';
