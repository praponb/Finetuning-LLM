SELECT id, items, ship_to, attributes, ship_to.zip AS ship_zip, SIZE(items) AS num_items, ship_to.country AS ship_country FROM carts
