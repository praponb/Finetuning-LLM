SELECT city, region FROM stores INTERSECT SELECT city, region FROM stores WHERE LENGTH(store_name) > 10
