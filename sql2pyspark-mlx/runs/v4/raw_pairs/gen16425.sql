SELECT s.city, s.store_name, s.sqft, o.channel, o.discount, o.order_date, INITCAP(TRIM(o.channel)) channel_title FROM stores s LEFT JOIN orders o ON s.id = o.store_id
