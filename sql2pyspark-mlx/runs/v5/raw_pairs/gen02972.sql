SELECT amount FROM orders WHERE status IN ('COMPLETED', 'CANCELLED') INTERSECT SELECT revenue FROM store_sales WHERE sale_date >= '2025-07-01';
