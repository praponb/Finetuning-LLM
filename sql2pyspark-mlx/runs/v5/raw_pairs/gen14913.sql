SELECT id, price, launch_date, category, ARRAY_CONTAINS(tags, 'eco') AS is_eco FROM products WHERE (price IS NOT NULL AND ARRAY_CONTAINS(tags, 'new')) OR launch_date <= '2025-01-31';
