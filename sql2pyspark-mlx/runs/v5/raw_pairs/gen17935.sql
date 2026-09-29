SELECT i.id, i.product_id, i.store_id, s.store_name, s.region, s.sqft, LAST_DAY(i.last_restock) AS last_restock_month_end FROM inventory i JOIN stores s ON i.store_id = s.id WHERE s.sqft <= 10454;
