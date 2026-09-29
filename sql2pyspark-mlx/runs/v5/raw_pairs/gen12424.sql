SELECT id, store_id FROM store_sales INTERSECT SELECT id, store_id FROM store_sales WHERE sale_date >= '2025-02-01';
