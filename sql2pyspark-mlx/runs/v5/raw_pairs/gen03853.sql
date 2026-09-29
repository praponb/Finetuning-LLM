SELECT id, coupon_codes, items, attributes, COALESCE(ship_to.zip, 'UNKNOWN') AS zip_or_unknown, ship_to.country AS ship_country, SIZE(attributes) AS num_attributes FROM carts;
