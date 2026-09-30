SELECT id, coupon_codes, attributes, ship_to.country AS ship_country, COALESCE(attributes['device'], 'none') AS device_or_default FROM carts;
