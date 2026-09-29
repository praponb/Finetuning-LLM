SELECT opened_date FROM stores UNION SELECT shipped_date AS opened_date FROM shipments WHERE shipped_date >= '2025-01-01' ORDER BY opened_date;
