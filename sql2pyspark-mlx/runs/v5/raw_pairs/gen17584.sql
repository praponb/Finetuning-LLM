select id, created_at, coupon_codes, ship_to, try_cast(attributes['weight_kg'] as double) as weight_kg, size(attributes) as num_attributes from carts
