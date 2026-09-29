SELECT id, store_id, revenue, ROUND(revenue / units, 2) revenue_per_unit FROM store_sales WHERE units BETWEEN 12 AND 21 OR units BETWEEN 21 AND 26;
