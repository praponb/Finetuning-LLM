SELECT id, customer_id, created_at, items, ship_to, attributes, ARRAY_MAX(TRANSFORM(items, x -> x.price)) AS max_item_price, attributes['device'] AS device FROM carts WHERE customer_id = 23;
