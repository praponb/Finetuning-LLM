SELECT cost, MONTH(shipped_date) AS shipped_date_month FROM shipments WHERE shipped_date >= '2025-02-01' ORDER BY shipped_date_month ASC
