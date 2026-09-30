SELECT id FROM payments INTERSECT SELECT id FROM payments WHERE paid_at >= '2024-09-01';
