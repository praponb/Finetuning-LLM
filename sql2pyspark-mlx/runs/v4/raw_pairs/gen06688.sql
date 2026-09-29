SELECT s.sqft, s.region, o.discount, o.customer_id, o.channel FROM stores s INNER JOIN orders o ON s.id = o.store_id WHERE s.opened_date > '2024-10-01'
