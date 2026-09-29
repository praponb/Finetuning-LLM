SELECT MIN(sqft) min_sqft, COUNT(*) row_count, COUNT(*) store_count FROM stores WHERE region IN ('West', 'Central')
