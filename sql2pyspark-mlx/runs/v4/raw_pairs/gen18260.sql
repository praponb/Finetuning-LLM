SELECT id, store_name, sqft FROM stores WHERE region NOT IN ('South', 'Central') AND (store_name IS NULL OR city = 'Berlin') ORDER BY id DESC LIMIT 5
