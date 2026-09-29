SELECT s.sqft, s.region, s.city, o.status, o.shipping_fee, o.amount FROM stores s JOIN orders o ON s.id = o.store_id WHERE s.region IN ('East', 'West') ORDER BY sqft DESC
