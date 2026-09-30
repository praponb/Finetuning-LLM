select id, customer_id, attr_key, value from carts lateral view outer explode(attributes) e as attr_key, value
