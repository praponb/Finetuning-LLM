SELECT id, paid_at, order_id, method, INITCAP(TRIM(status)) AS status_title FROM payments WHERE paid_at >= '2025-02-01' ORDER BY status_title ASC;
