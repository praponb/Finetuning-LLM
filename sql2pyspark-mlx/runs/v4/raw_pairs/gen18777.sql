SELECT status, INITCAP(TRIM(carrier)) AS carrier_title FROM shipments WHERE MONTH(delivered_date) IN (2, 3, 12) AND YEAR(shipped_date) = 2025
