SELECT launch_date FROM products WHERE category = 'Grocery' INTERSECT SELECT opened_date FROM stores WHERE region NOT IN ('Central', 'North')
