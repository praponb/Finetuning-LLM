SELECT shipped_date FROM shipments WHERE status != 'LOST' UNION SELECT shipped_date FROM shipments WHERE YEAR(shipped_date) >= 2024;
