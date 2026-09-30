SELECT amount value FROM payments INTERSECT SELECT cost value FROM shipments WHERE delivered_date < '2025-09-28';
